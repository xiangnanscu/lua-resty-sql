local nkeys = require "table.nkeys"
local clone = require "table.clone"
local encode = require("cjson").encode
local isempty = require "table.isempty"
local Array = require "resty.array"
local setmetatable = setmetatable
local ipairs = ipairs
local tostring = tostring
local type = type
local next = next
local pairs = pairs
local assert = assert
local error = error
local string_format = string.format
local table_concat = table.concat
local table_insert = table.insert
local table_new = table.new
local NULL = ngx.null
local match = ngx.re.match

local PG_SET_MAP = {
  _union = 'UNION',
  _union_all = 'UNION ALL',
  _except = 'EXCEPT',
  _except_all = 'EXCEPT ALL',
  _intersect = 'INTERSECT',
  _intersect_all = 'INTERSECT ALL'
}
local COMPARE_OPERATORS = { lt = "<", lte = "<=", gt = ">", gte = ">=", ne = "<>", eq = "=" }


---@param s string
---@return fun():string
local function make_token(s)
  local function raw_token()
    return s
  end

  return raw_token
end

local DEFAULT = make_token("DEFAULT")

local function map(tbl, func)
  local res = {}
  for i = 1, #tbl do
    res[i] = func(tbl[i])
  end
  return res
end

local function flat(tbl)
  local res = {}
  for i = 1, #tbl do
    local t = tbl[i]
    if type(t) ~= "table" then
      res[#res + 1] = t
    else
      for _, e in ipairs(flat(t)) do
        res[#res + 1] = e
      end
    end
  end
  return res
end

local function get_keys(rows)
  local columns = {}
  if rows[1] then
    local d = {}
    for _, row in ipairs(rows) do
      for k, _ in pairs(row) do
        if not d[k] then
          d[k] = true
          table_insert(columns, k)
        end
      end
    end
  else
    for k, _ in pairs(rows) do
      table_insert(columns, k)
    end
  end
  return columns
end

local INHERIT_METHODS = {
  new = true,
  __add = true,
  __sub = true,
  __mul = true,
  __div = true,
  __mod = true,
  __pow = true,
  __unm = true,
  __concat = true,
  __len = true,
  __eq = true,
  __lt = true,
  __le = true,
  __index = true,
  __newindex = true,
  __call = true,
  __tostring = true
}
local function class_new(cls, self)
  return setmetatable(self or {}, cls)
end

local function class__call(cls, attrs)
  local self = cls:new()
  self:init(attrs)
  return self
end

local function class__init(self, attrs)

end

---make a class with methods: __index, __call, class, new
---@param cls table
---@param parent table
---@return table
local function class(cls, parent)
  setmetatable(cls, parent)
  for method, _ in pairs(INHERIT_METHODS) do
    if cls[method] == nil and parent[method] ~= nil then
      cls[method] = parent[method]
    end
  end
  function cls.class(cls2, subcls)
    return class(subcls, cls2)
  end

  cls.new = cls.new or class_new
  cls.init = cls.init or class__init
  cls.__call = cls.__call or class__call
  cls.__index = cls
  return cls
end

local function get_foreign_object(attrs, prefix)
  -- when in : attrs = {id=1, buyer__name='tom', buyer__id=2}, prefix = 'buyer__'
  -- when out: attrs = {id=1}, fk_instance = {name='tom', id=2}
  local fk = {}
  local n = #prefix
  for k, v in pairs(attrs) do
    if k:sub(1, n) == prefix then
      fk[k:sub(n + 1)] = v
      attrs[k] = nil
    end
  end
  return fk
end

local function _prefix_with_V(column)
  return "V." .. column
end

---@param row any
---@return boolean
local function is_sql_instance(row)
  local meta = getmetatable(row)
  return meta and meta.__SQL_BUILDER__
end

local function _escape_factory(is_literal, is_bracket)
  ---value escaper for lua value
  ---@param value DBValue
  ---@return string
  local function as_sql_token(value)
    local value_type = type(value)
    if "string" == value_type then
      if is_literal then
        return "'" .. (value:gsub("'", "''")) .. "'"
      else
        return value
      end
    elseif "number" == value_type then
      return tostring(value)
    elseif "boolean" == value_type then
      return value and "TRUE" or "FALSE"
    elseif "function" == value_type then
      return value()
    elseif "table" == value_type then
      if is_sql_instance(value) then
        return "(" .. value:statement() .. ")"
      elseif value[1] ~= nil then
        local token = table_concat(map(value, as_sql_token), ", ")
        if is_bracket then
          return "(" .. token .. ")"
        else
          return token
        end
      else
        error("empty table as a Xodel value is not allowed")
      end
    elseif NULL == value then
      return 'NULL'
    else
      error(string_format("don't know how to escape value: %s (%s)", value, value_type))
    end
  end

  return as_sql_token
end
local as_literal = _escape_factory(true, true)
local as_literal_without_brackets = _escape_factory(true, false)
local as_token = _escape_factory(false, false)

local function get_list_tokens(a, b, ...)
  if b == nil then
    if type(a) == "table" then
      local tokens = {}
      for i = 1, #a do
        tokens[i] = a[i]
      end
      return as_token(tokens)
    elseif type(a) == "string" then
      return a
    else
      return as_token(a)
    end
  else
    local s = as_token(a) .. ", " .. as_token(b)
    for i = 1, select("#", ...) do
      local name = select(i, ...)
      s = s .. ", " .. as_token(name)
    end
    return s
  end
end

local function assemble_sql(opts)
  local statement
  if opts.update then
    local from = opts.from and " FROM " .. opts.from or ""
    local where = opts.where and " WHERE " .. opts.where or ""
    local returning = opts.returning and " RETURNING " .. opts.returning or ""
    statement = string_format("UPDATE %s SET %s%s%s%s", opts.table_name, opts.update, from, where, returning)
  elseif opts.insert then
    local returning = opts.returning and " RETURNING " .. opts.returning or ""
    statement = string_format("INSERT INTO %s %s%s", opts.table_name, opts.insert, returning)
  elseif opts.delete then
    local using = opts.using and " USING " .. opts.using or ""
    local where = opts.where and " WHERE " .. opts.where or ""
    local returning = opts.returning and " RETURNING " .. opts.returning or ""
    statement = string_format("DELETE FROM %s%s%s%s", opts.table_name, using, where, returning)
  else
    local from = opts.from or opts.table_name
    local where = opts.where and " WHERE " .. opts.where or ""
    local group = opts.group and " GROUP BY " .. opts.group or ""
    local having = opts.having and " HAVING " .. opts.having or ""
    local order = opts.order and " ORDER BY " .. opts.order or ""
    local limit = opts.limit and " LIMIT " .. opts.limit or ""
    local offset = opts.offset and " OFFSET " .. opts.offset or ""
    local distinct = opts.distinct and "DISTINCT " or
        opts.distinct_on and string_format("DISTINCT ON(%s) ", opts.distinct_on) or ""
    local select = opts.select or "*"
    statement = string_format("SELECT %s%s FROM %s%s%s%s%s%s%s",
      distinct, select, from, where, group, having, order, limit, offset)
  end
  if opts.with then
    return string_format("WITH %s %s", opts.with, statement)
  elseif opts.with_recursive then
    return string_format("WITH RECURSIVE %s %s", opts.with_recursive, statement)
  else
    return statement
  end
end

---@class Sql
---@field __index Sql
---@field __call fun(t:Sql, args:string|table):Sql
---@field __tostring fun(t:Sql):string
---@field class fun(cls:Sql, subcls:table, copy_parent?:boolean):Sql
---@field as_token fun(value:DBValue): string
---@field as_literal fun(value:DBValue): string
---@field as_literal_without_brackets fun(value:DBValue): string
---@field token fun(s:string): fun():string
---@field NULL userdata
---@field DEFAULT  fun():'DEFAULT'
---@field table_name string
---@field _pcall? boolean
---@field _as?  string
---@field _with?  string
---@field _with_recursive?  string
---@field _join?  string
---@field _distinct?  boolean
---@field _distinct_on?  string
---@field _returning?  string
---@field _returning_args?  DBValue[]
---@field _insert?  string
---@field _update?  string
---@field _delete?  boolean
---@field _using?  string
---@field _select?  string
---@field _from?  string
---@field _where?  string
---@field _group?  string
---@field _having?  string
---@field _order?  string
---@field _limit?  number
---@field _offset?  number
---@field _union?  Sql | string
---@field _union_all?  Sql | string
---@field _except?  Sql | string
---@field _except_all?  Sql | string
---@field _intersect?  Sql | string
---@field _intersect_all?  Sql | string
---@field _join_type?  string
---@field _prepend?  (Sql|string)[]
---@field _append?  (Sql|string)[]
local Sql = class({
  __SQL_BUILDER__ = true,
  NULL = NULL,
  DEFAULT = DEFAULT,
  token = make_token,
  as_token = as_token,
  as_literal = as_literal,
  as_literal_without_brackets = as_literal_without_brackets,
}, {
  __call = function(t, args)
    if type(args) == 'string' then
      return t:new { table_name = args }
    else
      return t:new(args)
    end
  end,
  __tostring = function(self)
    return self:statement()
  end
})

---@param cls Sql
---@param self? table
---@return Sql
function Sql.new(cls, self)
  return setmetatable(self or {}, cls)
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return Sql
function Sql._base_select(self, a, b, ...)
  local s = self:_base_get_select_token(a, b, ...)
  if not self._select then
    self._select = s
  elseif s ~= nil and s ~= "" then
    self._select = self._select .. ", " .. s
  end
  return self
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return string
function Sql._base_get_select_token(self, a, b, ...)
  if b == nil then
    if type(a) == 'table' then
      return Sql._base_get_select_token(self, unpack(a))
    else
      return as_token(a)
    end
  else
    local s = as_token(a) .. ", " .. as_token(b)
    for i = 1, select("#", ...) do
      s = s .. ", " .. as_token(select(i, ...))
    end
    return s
  end
end

---@param self Sql
---@param rows Records|Sql|string
---@param columns? string[]
---@return Sql
function Sql._base_insert(self, rows, columns)
  if type(rows) == "table" then
    if is_sql_instance(rows) then
      ---@cast rows Sql
      if rows._select then
        self:_set_select_subquery_insert_token(rows, columns)
      elseif rows._returning_args then
        self:_set_cud_subquery_insert_token(rows, columns)
      else
        error("select or returning args should be provided when inserting from a sub query")
      end
    elseif rows[1] then
      ---@cast rows Record[]
      self._insert = self:_get_bulk_insert_token(rows, columns)
    elseif next(rows) ~= nil then
      ---@cast rows Record
      self._insert = self:_get_insert_token(rows, columns)
    else
      error("can't pass empty table to Sql._base_insert")
    end
  elseif type(rows) == 'string' then
    self._insert = rows
  else
    error("invalid value type to Sql._base_insert:" .. type(rows))
  end
  return self
end

---@param self Sql
---@param row Record|string|Sql
---@param columns? string[]
---@return Sql
function Sql._base_update(self, row, columns)
  if is_sql_instance(row) then
    ---@cast row Sql
    self._update = self:_base_get_update_query_token(row, columns)
  elseif type(row) == "table" then
    self._update = self:_get_update_token(row, columns)
  else
    ---@cast row string
    self._update = row
  end
  return self
end

---@param self Sql
---@param join_type string
---@param right_table string
---@param key string
---@param op? string
---@param val? DBValue
---@return Sql
---@diagnostic disable-next-line: duplicate-set-field
function Sql._base_join_raw(self, join_type, right_table, key, op, val)
  local join_token = self:_get_join_token(join_type or "INNER", right_table, key, op, val)
  self._from = string_format("%s %s", self._from or self:get_table(), join_token)
  return self
end

---@param self Sql
---@param rows Record[]
---@param key Keys
---@param columns? string[]
---@return Sql
function Sql._base_merge(self, rows, key, columns)
  rows, columns = self:_get_cte_values_literal(rows, columns, false)
  local cte_name = string_format("V(%s)", table_concat(columns, ", "))
  local cte_values = string_format("(VALUES %s)", as_token(rows))
  local join_cond = self:_get_join_conditions(key, "V", "T")
  local vals_columns = map(columns, _prefix_with_V)
  local insert_subquery = Sql:new { table_name = "V" }
      :_base_select(vals_columns)
      :_base_join_raw("LEFT", "U AS T", join_cond)
      :_base_where_null("T." .. (key[1] or key))
  local updated_subquery
  if (type(key) == "table" and #key == #columns) or #columns == 1 then
    updated_subquery = Sql:new { table_name = "V" }
        :_base_select(vals_columns)
        :_base_join_raw("INNER", self.table_name .. " AS T", join_cond)
  else
    updated_subquery = Sql:new { table_name = self.table_name, _as = "T" }
        :_base_update(self:_get_update_token_with_prefix(columns, key, "V"))
        :_base_from("V"):_base_where(join_cond)
        :_base_returning(vals_columns)
  end
  self:with(cte_name, cte_values):with("U", updated_subquery)
  return Sql._base_insert(self, insert_subquery, columns)
end

---@param self Sql
---@param rows Sql|Record[]
---@param key Keys
---@param columns? string[]
---@return Sql
function Sql._base_upsert(self, rows, key, columns)
  assert(key, "you must provide key for upsert(string or table)")
  if is_sql_instance(rows) then
    assert(columns ~= nil, "you must specify columns when use subquery as values of upsert")
    self._insert = self:_get_upsert_query_token(rows, key, columns)
  elseif rows[1] then
    self._insert = self:_get_bulk_upsert_token(rows, key, columns)
  else
    self._insert = self:_get_upsert_token(rows, key, columns)
  end
  return self
end

---@param self Sql
---@param rows Record[]|Sql
---@param key Keys
---@param columns? string[]
---@return Sql
function Sql._base_updates(self, rows, key, columns)
  if is_sql_instance(rows) then
    ---@cast rows Sql
    columns = columns or flat(rows._returning_args)
    local cte_name = string_format("V(%s)", table_concat(columns, ", "))
    local join_cond = self:_get_join_conditions(key, "V", self._as or self.table_name)
    self:with(cte_name, rows)
    return Sql._base_update(self, self:_get_update_token_with_prefix(columns, key, "V"))
        :_base_from("V"):_base_where(join_cond)
  elseif #rows == 0 then
    error("empty rows passed to updates")
  else
    ---@cast rows Record[]
    rows, columns = self:_get_cte_values_literal(rows, columns, false)
    local cte_name = string_format("V(%s)", table_concat(columns, ", "))
    local cte_values = string_format("(VALUES %s)", as_token(rows))
    local join_cond = self:_get_join_conditions(key, "V", self._as or self.table_name)
    self:with(cte_name, cte_values)
    return Sql._base_update(self, self:_get_update_token_with_prefix(columns, key, "V"))
        :_base_from("V"):_base_where(join_cond)
  end
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return Sql
function Sql._base_returning(self, a, b, ...)
  local s = self:_base_get_select_token(a, b, ...)
  if not self._returning then
    self._returning = s
  elseif s ~= nil and s ~= "" then
    self._returning = self._returning .. ", " .. s
  else
    return self
  end
  if self._returning_args then
    self._returning_args = { self._returning_args, ... }
  else
    self._returning_args = { ... }
  end
  return self
end

---@param self Sql
---@param a string
---@param ... string
---@return Sql
function Sql._base_from(self, a, ...)
  if not self._from then
    self._from = self:_base_get_select_token(a, ...)
  else
    self._from = self._from .. ", " .. self:_base_get_select_token(a, ...)
  end
  return self
end

---@param self Sql
---@param join_type string
---@param join_args table|string
---@param key string|function
---@param op? string|function
---@param val? DBValue
---@return Sql
function Sql._base_join(self, join_type, join_args, key, op, val)
  if type(key) == 'function' then
    self:_register_join_model({
      model = self.model,
      fk_model = join_args,
      join_callback = key,
      sql_callback = op,
    }, join_type)
    return self
  elseif type(join_args) == 'table' then
    self:_register_join_model(join_args, join_type)
    return self
  end
  local fk = self.model.foreign_keys[join_args]
  if fk then
    self:_register_join_model({
      model = self.model,
      column = join_args,
      fk_model = fk.reference,
      fk_column = fk.reference_column,
      fk_alias = fk.reference.table_name
    }, join_type)
    return self
  else
    ---@cast op string
    local join_token = self:_get_join_token(join_type or "INNER", join_args, key, op, val)
    self._from = string_format("%s %s", self._from or self:get_table(), join_token)
    return self
  end
end

---@param self Sql
---@param cond table|string|function
---@param op? string
---@param dval? DBValue
---@return Sql
function Sql._base_where(self, cond, op, dval)
  local where_token = self:_base_get_condition_token(cond, op, dval)
  return self:_handle_where_token(where_token, "(%s) AND (%s)")
end

---@param self Sql
---@param kwargs {[string|number]:any}
---@param logic? "AND"|"OR"
---@return string
function Sql._base_get_condition_token_from_table(self, kwargs, logic)
  local tokens = {}
  for k, value in pairs(kwargs) do
    if type(k) == "string" then
      tokens[#tokens + 1] = string_format("%s = %s", k, as_literal(value))
    else
      local token = self:_base_get_condition_token(value)
      if token ~= nil and token ~= "" then
        tokens[#tokens + 1] = '(' .. token .. ')'
      end
    end
  end
  if logic == nil then
    return table_concat(tokens, " AND ")
  else
    return table_concat(tokens, " " .. logic .. " ")
  end
end

---@param self Sql
---@param cond table|string|function
---@param op? DBValue
---@param dval? DBValue
---@return string
function Sql._base_get_condition_token(self, cond, op, dval)
  if op == nil then
    local argtype = type(cond)
    if argtype == "table" then
      return Sql._base_get_condition_token_from_table(self, cond)
    elseif argtype == "string" then
      --**mind injection here
      return cond
    elseif argtype == "function" then
      local old_where = self._where
      self._where = nil
      local res, err = cond(self)
      if res ~= nil then
        if res == self then
          local group_where = self._where
          if group_where == nil then
            error("no where token generate after calling condition function")
          else
            self._where = old_where
            return group_where
          end
        else
          self._where = old_where
          return res
        end
      else
        error(err or "nil returned in condition function")
      end
    else
      error("invalid condition type: " .. argtype)
    end
  elseif dval == nil then
    return string_format("%s = %s", cond, as_literal(op))
  else
    return string_format("%s %s %s", cond, op, as_literal(dval))
  end
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql._base_where_in(self, cols, range)
  local in_token = self:_get_in_token(cols, range)
  if self._where then
    self._where = string_format("(%s) AND %s", self._where, in_token)
  else
    self._where = in_token
  end
  return self
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql._base_where_not_in(self, cols, range)
  local not_in_token = self:_get_in_token(cols, range, "NOT IN")
  if self._where then
    self._where = string_format("(%s) AND %s", self._where, not_in_token)
  else
    self._where = not_in_token
  end
  return self
end

---@param self Sql
---@param col string
---@return Sql
function Sql._base_where_null(self, col)
  if self._where then
    self._where = string_format("(%s) AND %s IS NULL", self._where, col)
  else
    self._where = col .. " IS NULL"
  end
  return self
end

---@param self Sql
---@param col string
---@return Sql
function Sql._base_where_not_null(self, col)
  if self._where then
    self._where = string_format("(%s) AND %s IS NOT NULL", self._where, col)
  else
    self._where = col .. " IS NOT NULL"
  end
  return self
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql._base_where_between(self, col, low, high)
  if self._where then
    self._where = string_format("(%s) AND (%s BETWEEN %s AND %s)", self._where, col, low, high)
  else
    self._where = string_format("%s BETWEEN %s AND %s", col, low, high)
  end
  return self
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql._base_where_not_between(self, col, low, high)
  if self._where then
    self._where = string_format("(%s) AND (%s NOT BETWEEN %s AND %s)", self._where, col, low, high)
  else
    self._where = string_format("%s NOT BETWEEN %s AND %s", col, low, high)
  end
  return self
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql._base_or_where_in(self, cols, range)
  local in_token = self:_get_in_token(cols, range)
  if self._where then
    self._where = string_format("%s OR %s", self._where, in_token)
  else
    self._where = in_token
  end
  return self
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql._base_or_where_not_in(self, cols, range)
  local not_in_token = self:_get_in_token(cols, range, "NOT IN")
  if self._where then
    self._where = string_format("%s OR %s", self._where, not_in_token)
  else
    self._where = not_in_token
  end
  return self
end

---@param self Sql
---@param col string
---@return Sql
function Sql._base_or_where_null(self, col)
  if self._where then
    self._where = string_format("%s OR %s IS NULL", self._where, col)
  else
    self._where = col .. " IS NULL"
  end
  return self
end

---@param self Sql
---@param col string
---@return Sql
function Sql._base_or_where_not_null(self, col)
  if self._where then
    self._where = string_format("%s OR %s IS NOT NULL", self._where, col)
  else
    self._where = col .. " IS NOT NULL"
  end
  return self
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql._base_or_where_between(self, col, low, high)
  if self._where then
    self._where = string_format("%s OR (%s BETWEEN %s AND %s)", self._where, col, low, high)
  else
    self._where = string_format("%s BETWEEN %s AND %s", col, low, high)
  end
  return self
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql._base_or_where_not_between(self, col, low, high)
  if self._where then
    self._where = string_format("%s OR (%s NOT BETWEEN %s AND %s)", self._where, col, low, high)
  else
    self._where = string_format("%s NOT BETWEEN %s AND %s", col, low, high)
  end
  return self
end

---@param self Sql
---@return Sql
function Sql.pcall(self)
  self._pcall = true
  return self
end

---@param self Sql
---@param err ValidateError
---@param level? integer
---@return nil, ValidateError?
function Sql.error(self, err, level)
  if self._pcall then
    return nil, err
  else
    error(err, level)
  end
end

---@param self Sql
---@param rows Record[]
---@param columns string[]
---@return DBValue[][]
function Sql._rows_to_array(self, rows, columns)
  local c = #columns
  local n = #rows
  local res = table_new(n, 0)
  local fields = self.model.fields
  for i = 1, n do
    res[i] = table_new(c, 0)
  end
  for i, col in ipairs(columns) do
    for j = 1, n do
      local v = rows[j][col]
      if v ~= nil and v ~= '' then
        res[j][i] = v
      elseif fields[col] then
        local default = fields[col].default
        if default ~= nil then
          res[j][i] = fields[col]:get_default(rows[j])
        else
          res[j][i] = NULL
        end
      else
        res[j][i] = NULL
      end
    end
  end
  return res
end

---@param self Sql
---@param row Record
---@param columns? string[]
---@return string[], string[]
function Sql._get_insert_values_token(self, row, columns)
  local value_list = {}
  if not columns then
    columns = {}
    for k, v in pairs(row) do
      table_insert(columns, k)
      table_insert(value_list, v)
    end
  else
    for _, col in pairs(columns) do
      local v = row[col]
      if v ~= nil then
        table_insert(value_list, v)
      else
        table_insert(value_list, DEFAULT)
      end
    end
  end
  return value_list, columns
end

---@param self Sql
---@param rows Record[]
---@param columns? string[]
---@return string[], string[]
function Sql._get_bulk_insert_values_token(self, rows, columns)
  columns = columns or get_keys(rows)
  rows = self:_rows_to_array(rows, columns)
  return map(rows, as_literal), columns
end

---@param self Sql
---@param columns string[]
---@param key Keys
---@param table_name string
---@return string
function Sql._get_update_token_with_prefix(self, columns, key, table_name)
  local tokens = {}
  if type(key) == "string" then
    for i, col in ipairs(columns) do
      if col ~= key then
        table_insert(tokens, string_format("%s = %s.%s", col, table_name, col))
      end
    end
  else
    local sets = {}
    for i, k in ipairs(key) do
      sets[k] = true
    end
    for i, col in ipairs(columns) do
      if not sets[col] then
        table_insert(tokens, string_format("%s = %s.%s", col, table_name, col))
      end
    end
  end
  return table_concat(tokens, ", ")
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return string
function Sql._get_select_token(self, a, b, ...)
  if b == nil then
    if type(a) == "table" then
      local tokens = {}
      for i = 1, #a do
        tokens[i] = self:_get_select_column(a[i])
      end
      return as_token(tokens)
    elseif type(a) == "string" then
      return self:_get_select_column(a) --[[@as string]]
    else
      return as_token(a)
    end
  else
    a = self:_get_select_column(a)
    b = self:_get_select_column(b)
    local s = as_token(a) .. ", " .. as_token(b)
    for i = 1, select("#", ...) do
      local name = select(i, ...)
      s = s .. ", " .. as_token(self:_get_select_column(name))
    end
    return s
  end
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return string
function Sql._get_select_token_literal(self, a, b, ...)
  if b == nil then
    if type(a) == "table" then
      local tokens = {}
      for i = 1, #a do
        tokens[i] = as_literal(a[i])
      end
      return as_token(tokens)
    else
      return as_literal(a)
    end
  else
    local s = as_literal(a) .. ", " .. as_literal(b)
    for i = 1, select("#", ...) do
      local name = select(i, ...)
      s = s .. ", " .. as_literal(name)
    end
    return s
  end
end

---@param self Sql
---@param row Record
---@param columns? string[]
---@return string
function Sql._get_update_token(self, row, columns)
  local kv = {}
  if not columns then
    for k, v in pairs(row) do
      table_insert(kv, string_format("%s = %s", k, as_literal(v)))
    end
  else
    for _, k in ipairs(columns) do
      local v = row[k]
      table_insert(kv, string_format("%s = %s", k, v ~= nil and as_literal(v) or 'DEFAULT'))
    end
  end
  return table_concat(kv, ", ")
end

---@param self Sql
---@param name string
---@param token? Sql|DBValue
---@return string
function Sql._get_with_token(self, name, token)
  if token == nil then
    return name
  elseif is_sql_instance(token) then
    ---@cast token Sql
    return string_format("%s AS (%s)", name, token:statement())
  else
    return string_format("%s AS %s", name, token)
  end
end

---@param self Sql
---@param row Record
---@param columns? string[]
---@return string
function Sql._get_insert_token(self, row, columns)
  local values_list, insert_columns = self:_get_insert_values_token(row, columns)
  return string_format("(%s) VALUES %s", as_token(insert_columns), as_literal(values_list))
end

---@param self Sql
---@param rows Record[]
---@param columns? string[]
---@return string
function Sql._get_bulk_insert_token(self, rows, columns)
  rows, columns = self:_get_bulk_insert_values_token(rows, columns)
  return string_format("(%s) VALUES %s", as_token(columns), as_token(rows))
end

---@param self Sql
---@param sub_query Sql
---@param columns? string[]
function Sql._set_select_subquery_insert_token(self, sub_query, columns)
  local columns_token = as_token(columns or sub_query._select or "")
  if columns_token ~= "" then
    self._insert = string_format("(%s) %s", columns_token, sub_query:statement())
  else
    self._insert = sub_query:statement()
  end
end

---@param self Sql
---@param sub_query Sql
function Sql._set_cud_subquery_insert_token(self, sub_query, columns)
  local insert_columns = columns or flat(sub_query._returning_args)
  local cud_select_query = Sql:new { table_name = "d" }:_base_select(insert_columns)
  self:with(string_format("d(%s)", as_token(insert_columns)), sub_query)
  self._insert = string_format("(%s) %s", as_token(insert_columns), cud_select_query:statement())
end

---@param self Sql
---@param row Record
---@param key Keys
---@param columns? string[]
---@return string
function Sql._get_upsert_token(self, row, key, columns)
  local values_list, insert_columns = self:_get_insert_values_token(row, columns)
  local insert_token = string_format("(%s) VALUES %s ON CONFLICT (%s)",
    as_token(insert_columns),
    as_literal(values_list),
    self:_get_select_token(key))
  if (type(key) == "table" and #key == #insert_columns) or #insert_columns == 1 then
    return string_format("%s DO NOTHING", insert_token)
  else
    return string_format("%s DO UPDATE SET %s", insert_token,
      self:_get_update_token_with_prefix(insert_columns, key, "EXCLUDED"))
  end
end

---@param self Sql
---@param rows Record[]
---@param key Keys
---@param columns? string[]
---@return string
function Sql._get_bulk_upsert_token(self, rows, key, columns)
  rows, columns = self:_get_bulk_insert_values_token(rows, columns)
  local insert_token = string_format("(%s) VALUES %s ON CONFLICT (%s)", as_token(columns), as_token(rows),
    self:_base_get_select_token(key))
  if (type(key) == "table" and #key == #columns) or #columns == 1 then
    return string_format("%s DO NOTHING", insert_token)
  else
    return string_format("%s DO UPDATE SET %s", insert_token,
      self:_get_update_token_with_prefix(columns, key, "EXCLUDED"))
  end
end

---@param self Sql
---@param rows Sql
---@param key Keys
---@param columns string[]
---@return string
function Sql._get_upsert_query_token(self, rows, key, columns)
  local columns_token = self:_get_select_token(columns)
  local insert_token = string_format("(%s) %s ON CONFLICT (%s)", columns_token, rows:statement(),
    self:_get_select_token(key))
  if (type(key) == "table" and #key == #columns) or #columns == 1 then
    return string_format("%s DO NOTHING", insert_token)
  else
    return string_format("%s DO UPDATE SET %s", insert_token,
      self:_get_update_token_with_prefix(columns, key, "EXCLUDED"))
  end
end

---@param self Sql
---@param key string
---@param op? string
---@param val? DBValue
---@return string
function Sql._get_join_expr(self, key, op, val)
  if op == nil then
    return key
  elseif val == nil then
    return string_format("%s = %s", key, op)
  else
    return string_format("%s %s %s", key, op, val)
  end
end

---@param self Sql
---@param join_type JOIN_TYPE
---@param right_table string
---@param key string
---@param op? string
---@param val? DBValue
---@return string
function Sql._get_join_token(self, join_type, right_table, key, op, val)
  if key ~= nil then
    return string_format("%s JOIN %s ON (%s)", join_type, right_table, self:_get_join_expr(key, op, val))
  else
    return string_format("%s JOIN %s", join_type, right_table)
  end
end

---@param self Sql
---@param cols Keys
---@param range Sql|table|string
---@param op? string
---@return string
function Sql._get_in_token(self, cols, range, op)
  cols = as_token(cols)
  op = op or "IN"
  if type(range) == 'table' then
    if is_sql_instance(range) then
      return string_format("(%s) %s (%s)", cols, op, range:statement())
    else
      return string_format("(%s) %s %s", cols, op, as_literal(range))
    end
  else
    return string_format("(%s) %s %s", cols, op, range)
  end
end

---@param self Sql
---@param sub_select Sql
---@param columns? string[]
---@return string
function Sql._get_update_query_token(self, sub_select, columns)
  local columns_token = columns and self:_get_select_token(columns) or sub_select._select
  return string_format("(%s) = (%s)", columns_token, sub_select:statement())
end

---@param self Sql
---@param sub_select Sql
---@param columns? string[]
---@return string
function Sql._base_get_update_query_token(self, sub_select, columns)
  local columns_token = columns and self:_base_get_select_token(columns) or sub_select._select
  return string_format("(%s) = (%s)", columns_token, sub_select:statement())
end

---@param self Sql
---@param key Keys
---@param left_table string
---@param right_table string
---@return string
function Sql._get_join_conditions(self, key, left_table, right_table)
  if type(key) == "string" then
    return string_format("%s.%s = %s.%s", left_table, key, right_table, key)
  end
  local res = {}
  for _, k in ipairs(key) do
    res[#res + 1] = string_format("%s.%s = %s.%s", left_table, k, right_table, k)
  end
  return table_concat(res, " AND ")
end

---@param self Sql
---@param join_type JOIN_TYPE
---@param join_table string
---@param join_cond string
function Sql._handle_join(self, join_type, join_table, join_cond)
  if self._update then
    self:_base_from(join_table)
    self:_base_where(join_cond)
  elseif self._delete then
    self._using = join_table
    self:_base_where(join_cond)
  else
    self:_base_join(join_type, join_table, join_cond)
  end
end

function Sql._parse_column(self, key, as_select)
  local a, b = key:find("__", 1, true)
  if not a then
    if as_select then
      return key
    else
      return key, "eq"
    end
  end
  local e = key:sub(1, a - 1)
  local op = key:sub(b + 1)
  return e, op
end

---@param self Sql
---@param key DBValue
---@return DBValue
function Sql._get_select_column(self, key)
  if type(key) ~= 'string' then
    return key
  else
    return (self:_parse_column(key, true))
  end
end

---@param self Sql
---@return integer
function Sql._get_join_number(self)
  if self._join_keys then
    return nkeys(self._join_keys) + 1
  else
    return 1
  end
end

---@param self Sql
---@param where_token string
---@param tpl string
---@return Sql
function Sql._handle_where_token(self, where_token, tpl)
  if where_token == "" then
    return self
  elseif self._where == nil then
    self._where = where_token
  else
    self._where = string_format(tpl, self._where, where_token)
  end
  return self
end

---@param self Sql
---@param kwargs {[string|number]:any}
---@param logic? string
---@return string
function Sql._get_condition_token_from_table(self, kwargs, logic)
  local tokens = {}
  for k, value in pairs(kwargs) do
    if type(k) == "string" then
      tokens[#tokens + 1] = self:_get_expr_token(value, self:_parse_column(k, false))
    else
      local token = self:_get_condition_token(value)
      if token ~= nil and token ~= "" then
        tokens[#tokens + 1] = '(' .. token .. ')'
      end
    end
  end
  if logic == nil then
    return table_concat(tokens, " AND ")
  else
    return table_concat(tokens, " " .. logic .. " ")
  end
end

---@param self Sql
---@param cond table|string|function
---@param op? DBValue
---@param dval? DBValue
---@return string
function Sql._get_condition_token(self, cond, op, dval)
  if op == nil then
    if type(cond) == 'table' then
      return Sql._get_condition_token_from_table(self, cond)
    else
      return Sql._base_get_condition_token(self, cond)
    end
  elseif dval == nil then
    ---@cast cond string
    return string_format("%s = %s", self:_get_column(cond), as_literal(op))
  else
    ---@cast cond string
    return string_format("%s %s %s", self:_get_column(cond), op, as_literal(dval))
  end
end

---@param self Sql
---@param cond table|string|function
---@param op? DBValue
---@param dval? DBValue
---@return string
function Sql._get_condition_token_or(self, cond, op, dval)
  if type(cond) == "table" then
    return self:_get_condition_token_from_table(cond, "OR")
  else
    return self:_get_condition_token(cond, op, dval)
  end
end

---@param self Sql
---@param cond table|string|function
---@param op? DBValue
---@param dval? DBValue
---@return string
function Sql._get_condition_token_not(self, cond, op, dval)
  local token
  if type(cond) == "table" then
    token = self:_get_condition_token_from_table(cond, "OR")
  else
    token = self:_get_condition_token(cond, op, dval)
  end
  return token ~= "" and string_format("NOT (%s)", token) or ""
end

---@param self Sql
---@param other_sql Sql
---@param set_operation_attr SqlSet
---@return Sql
function Sql._handle_set_option(self, other_sql, set_operation_attr)
  if not self[set_operation_attr] then
    self[set_operation_attr] = other_sql:statement();
  else
    self[set_operation_attr] = string_format("(%s) %s (%s)", self[set_operation_attr], PG_SET_MAP[set_operation_attr],
      other_sql:statement());
  end
  if self ~= Sql then
    self.statement = self._statement_for_set
  else
    error("don't call _handle_set_option directly on Sql class")
  end
  return self;
end

---@param self Sql
---@return string
function Sql._statement_for_set(self)
  local statement = Sql.statement(self)
  if self._intersect then
    statement = string_format("(%s) INTERSECT (%s)", statement, self._intersect)
  elseif self._intersect_all then
    statement = string_format("(%s) INTERSECT ALL (%s)", statement, self._intersect_all)
  elseif self._union then
    statement = string_format("(%s) UNION (%s)", statement, self._union)
  elseif self._union_all then
    statement = string_format("%s UNION ALL (%s)", statement, self._union_all)
  elseif self._except then
    statement = string_format("(%s) EXCEPT (%s)", statement, self._except)
  elseif self._except_all then
    statement = string_format("(%s) EXCEPT ALL (%s)", statement, self._except_all)
  end
  return statement
end

---@param self Sql
---@param ... Sql[]|string[]
---@return Sql
function Sql.prepend(self, ...)
  if not self._prepend then
    self._prepend = {}
  end
  for _, statement in ipairs({ ... }) do
    self._prepend[#self._prepend + 1] = statement
  end
  return self
end

---@param self Sql
---@param ... Sql[]|string[]
---@return Sql
function Sql.append(self, ...)
  if not self._append then
    self._append = {}
  end
  for _, statement in ipairs({ ... }) do
    self._append[#self._append + 1] = statement
  end
  return self
end

---@param self Sql
---@return string
function Sql.statement(self)
  local table_name = self:get_table()
  local statement = assemble_sql {
    table_name = table_name,
    with = self._with,
    with_recursive = self._with_recursive,
    join = self._join,
    distinct = self._distinct,
    distinct_on = self._distinct_on,
    returning = self._returning,
    insert = self._insert,
    update = self._update,
    delete = self._delete,
    using = self._using,
    select = self._select,
    from = self._from,
    where = self._where,
    group = self._group,
    having = self._having,
    order = self._order,
    limit = self._limit,
    offset = self._offset
  }
  if self._prepend then
    local res = {}
    for _, sql in ipairs(self._prepend) do
      if type(sql) == 'string' then
        res[#res + 1] = sql
      else
        res[#res + 1] = sql:statement()
      end
    end
    statement = table_concat(res, ';') .. ';' .. statement
  end
  if self._append then
    local res = {}
    for _, sql in ipairs(self._append) do
      if type(sql) == 'string' then
        res[#res + 1] = sql
      else
        res[#res + 1] = sql:statement()
      end
    end
    statement = statement .. ';' .. table_concat(res, ';')
  end
  return statement
end

---@param self Sql
---@param name string
---@param token? DBValue
---@return Sql
function Sql.with(self, name, token)
  local with_token = self:_get_with_token(name, token)
  if self._with then
    self._with = string_format("%s, %s", self._with, with_token)
  else
    self._with = with_token
  end
  return self
end

---@param self Sql
---@param name string
---@param token? DBValue
---@return Sql
function Sql.with_recursive(self, name, token)
  local with_token = self:_get_with_token(name, token)
  if self._with_recursive then
    self._with_recursive = string_format("%s, %s", self._with_recursive, with_token)
  else
    self._with_recursive = with_token
  end
  return self
end

---@param self Sql
---@param other_sql Sql
---@return Sql
function Sql.union(self, other_sql)
  return self:_handle_set_option(other_sql, "_union");
end

---@param self Sql
---@param other_sql Sql
---@return Sql
function Sql.union_all(self, other_sql)
  return self:_handle_set_option(other_sql, "_union_all");
end

---@param self Sql
---@param other_sql Sql
---@return Sql
function Sql.except(self, other_sql)
  return self:_handle_set_option(other_sql, "_except");
end

---@param self Sql
---@param other_sql Sql
---@return Sql
function Sql.except_all(self, other_sql)
  return self:_handle_set_option(other_sql, "_except_all");
end

---@param self Sql
---@param other_sql Sql
---@return Sql
function Sql.intersect(self, other_sql)
  return self:_handle_set_option(other_sql, "_intersect");
end

---@param self Sql
---@param other_sql Sql
---@return Sql
function Sql.intersect_all(self, other_sql)
  return self:_handle_set_option(other_sql, "_intersect_all");
end

---@param self Sql
---@param table_alias string
---@return Sql
function Sql.as(self, table_alias)
  self._as = table_alias
  return self
end

---@param self Sql
---@param name string
---@param rows Record[]
---@return Sql
function Sql.with_values(self, name, rows)
  local columns = get_keys(rows[1])
  rows, columns = self:_get_cte_values_literal(rows, columns, true)
  local cte_name = string_format("%s(%s)", name, table_concat(columns, ", "))
  local cte_values = string_format("(VALUES %s)", as_token(rows))
  return self:with(cte_name, cte_values)
end

---@param self Sql
---@param rows Record[]
---@param key Keys
---@return Sql|XodelInstance[]
function Sql.get_merge(self, rows, key)
  local columns = get_keys(rows[1])
  rows, columns = self:_get_cte_values_literal(rows, columns, true)
  local join_cond = self:_get_join_conditions(key, "V", self._as or self.table_name)
  local cte_name = string_format("V(%s)", table_concat(columns, ", "))
  local cte_values = string_format("(VALUES %s)", as_token(rows))
  self:_base_select("V.*"):with(cte_name, cte_values):_base_join("RIGHT", "V", join_cond)
  return self
end

---@param self Sql
---@return Sql
function Sql.copy(self)
  local copy_sql = {}
  for key, value in pairs(self) do
    if type(value) == 'table' then
      copy_sql[key] = clone(value)
    else
      copy_sql[key] = value
    end
  end
  return setmetatable(copy_sql, getmetatable(self))
end

---@param self Sql
---@param cond? table|string|function
---@param op? string
---@param dval? DBValue
---@return Sql
function Sql.delete(self, cond, op, dval)
  self._delete = true
  if cond ~= nil then
    self:where(cond, op, dval)
  end
  return self
end

---@param self Sql
---@return Sql
function Sql.distinct(self)
  self._distinct = true
  return self
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return Sql
function Sql.select(self, a, b, ...)
  local s = self:_get_select_token(a, b, ...)
  if not self._select then
    self._select = s
  elseif s ~= nil and s ~= "" then
    self._select = self._select .. ", " .. s
  end
  return self
end

---@param self Sql
---@param key string
---@param alias string
---@return Sql
function Sql.select_as(self, key, alias)
  local col = self:_parse_column(key, true, true) .. ' AS ' .. alias
  if not self._select then
    self._select = col
  else
    self._select = self._select .. ", " .. col
  end
  return self
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return Sql
function Sql.select_literal(self, a, b, ...)
  local s = self:_get_select_token_literal(a, b, ...)
  if not self._select then
    self._select = s
  elseif s ~= nil and s ~= "" then
    self._select = self._select .. ", " .. s
  end
  return self
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return Sql
function Sql.returning(self, a, b, ...)
  local s = self:_get_select_token(a, b, ...)
  if not self._returning then
    self._returning = s
  elseif s ~= nil and s ~= "" then
    self._returning = self._returning .. ", " .. s
  else
    return self
  end
  if self._returning_args then
    self._returning_args = { self._returning_args, a, b, ... }
  else
    self._returning_args = { a, b, ... }
  end
  return self
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return Sql
function Sql.returning_literal(self, a, b, ...)
  local s = self:_get_select_token_literal(a, b, ...)
  if not self._returning then
    self._returning = s
  elseif s ~= nil and s ~= "" then
    self._returning = self._returning .. ", " .. s
  end
  --**this should be _returning_literal_args ?
  if self._returning_args then
    self._returning_args = { self._returning_args, a, b, ... }
  else
    self._returning_args = { a, b, ... }
  end
  return self
end

function Sql.group(self, ...)
  if not self._group then
    self._group = self:_get_select_token(...)
  else
    self._group = self._group .. ", " .. self:_get_select_token(...)
  end
  return self
end

function Sql.group_by(self, ...) return self:group(...) end

---@param self Sql
---@param key DBValue
---@return DBValue
function Sql._get_order_column(self, key)
  if type(key) ~= 'string' then
    return self:_get_select_column(key)
  else
    local matched = match(key, '^([-+])?([\\w_.]+)$', 'josui')
    if matched then
      return string_format("%s %s", self:_get_select_column(matched[2]), matched[1] == '-' and 'DESC' or 'ASC')
    else
      error(string_format("invalid order arg format: %s", key))
      -- return self:_get_select_column(key)
    end
  end
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return string
function Sql._get_order_token(self, a, b, ...)
  if b == nil then
    if type(a) == "table" then
      local tokens = {}
      for i = 1, #a do
        tokens[i] = self:_get_order_column(a[i])
      end
      return as_token(tokens)
    elseif type(a) == "string" then
      return self:_get_order_column(a) --[[@as string]]
    else
      return as_token(a)
    end
  else
    a = self:_get_order_column(a)
    b = self:_get_order_column(b)
    local s = as_token(a) .. ", " .. as_token(b)
    for i = 1, select("#", ...) do
      local name = select(i, ...)
      s = s .. ", " .. as_token(self:_get_order_column(name))
    end
    return s
  end
end

---@param self Sql
---@param ...? DBValue
---@return Sql
function Sql.order(self, ...)
  if not self._order then
    self._order = self:_get_order_token(...)
  else
    self._order = self._order .. ", " .. self:_get_order_token(...)
  end
  return self
end

function Sql.order_by(self, ...) return self:order(...) end

---@param self Sql
---@param a string
---@param ... string
---@return Sql
function Sql.using(self, a, ...)
  self._delete = true
  self._using = self:_get_select_token(a, ...)
  return self
end

---@param self Sql
---@param a string
---@param ... string
---@return Sql
function Sql.from(self, a, ...)
  if not self._from then
    self._from = get_list_tokens(a, ...)
  else
    self._from = self._from .. ", " .. get_list_tokens(a, ...)
  end
  return self
end

---@param self Sql
---@return string?
function Sql.get_table(self)
  if self.table_name == nil then
    return nil
  elseif self._as ~= nil then
    return self.table_name .. ' AS ' .. self._as
  else
    return self.table_name
  end
end

---@param self Sql
---@param join_args string
---@param key string
---@param op? string
---@param val? DBValue
---@return Sql
function Sql.join(self, join_args, key, op, val)
  return self:_base_join("INNER", join_args, key, op, val)
end

---@param self Sql
---@param join_args string
---@param key string
---@param op? string
---@param val? DBValue
---@return Sql
function Sql.inner_join(self, join_args, key, op, val)
  return self:_base_join("INNER", join_args, key, op, val)
end

---@param self Sql
---@param join_args string
---@param key string
---@param op? string
---@param val? DBValue
---@return Sql
function Sql.left_join(self, join_args, key, op, val)
  return self:_base_join("LEFT", join_args, key, op, val)
end

---@param self Sql
---@param join_args string
---@param key string
---@param op? string
---@param val? DBValue
---@return Sql
function Sql.right_join(self, join_args, key, op, val)
  return self:_base_join("RIGHT", join_args, key, op, val)
end

---@param self Sql
---@param join_args string
---@param key string
---@param op string
---@param val DBValue
---@return Sql
function Sql.full_join(self, join_args, key, op, val)
  return self:_base_join("FULL", join_args, key, op, val)
end

---@param self Sql
---@param join_args string
---@param key string
---@param op string
---@param val DBValue
---@return Sql
function Sql.cross_join(self, join_args, key, op, val)
  return self:_base_join("CROSS", join_args, key, op, val)
end

---@param self Sql
---@param n integer
---@return Sql
function Sql.limit(self, n)
  self._limit = n
  return self
end

---@param self Sql
---@param n integer
---@return Sql
function Sql.offset(self, n)
  self._offset = n
  return self
end

---@param self Sql
---@param cond table|string|function
---@param op? string
---@param dval? DBValue
---@return Sql
function Sql.where(self, cond, op, dval)
  local where_token = self:_get_condition_token(cond, op, dval)
  return self:_handle_where_token(where_token, "(%s) AND (%s)")
end

local logic_priority = { ['init'] = 0, ['or'] = 1, ['and'] = 2, ['not'] = 3, ['OR'] = 1, ['AND'] = 2, ['NOT'] = 3 }
---@param self Sql
---@param cond table
---@param father_op string
---@return string
function Sql.parse_where_exp(self, cond, father_op)
  local logic_op = cond[1]
  local tokens = {}
  for i = 2, #cond do
    local value = cond[i]
    if value[1] then
      tokens[#tokens + 1] = self:parse_where_exp(value, logic_op)
    else
      for k, v in pairs(value) do
        tokens[#tokens + 1] = self:_get_expr_token(v, self:_parse_column(k, false))
      end
    end
  end
  local where_token
  if logic_op == 'not' or logic_op == 'NOT' then
    where_token = 'NOT ' .. table_concat(tokens, " AND NOT ")
  else
    where_token = table_concat(tokens, string_format(" %s ", logic_op))
  end

  if logic_priority[logic_op] < logic_priority[father_op] then
    return "(" .. where_token .. ")"
  else
    return where_token
  end
end

---@param self Sql
---@param cond table
---@return Sql
function Sql.where_exp(self, cond)
  local where_token = self:parse_where_exp(cond, 'init')
  return self:_handle_where_token(where_token, "(%s) AND (%s)")
end

---@param self Sql
---@param cond table|string|function
---@param op? string
---@param dval? DBValue
---@return Sql
function Sql.where_or(self, cond, op, dval)
  local where_token = self:_get_condition_token_or(cond, op, dval)
  return self:_handle_where_token(where_token, "(%s) AND (%s)")
end

---@param self Sql
---@param cond table|string|function
---@param op? string
---@param dval? DBValue
---@return Sql
function Sql.or_where_or(self, cond, op, dval)
  local where_token = self:_get_condition_token_or(cond, op, dval)
  return self:_handle_where_token(where_token, "%s OR %s")
end

---@param self Sql
---@param cond table|string|function
---@param op? string
---@param dval? DBValue
---@return Sql
function Sql.where_not(self, cond, op, dval)
  local where_token = self:_get_condition_token_not(cond, op, dval)
  return self:_handle_where_token(where_token, "(%s) AND (%s)")
end

---@param self Sql
---@param cond table|string|function
---@param op? string
---@param dval? DBValue
---@return Sql
function Sql.or_where(self, cond, op, dval)
  local where_token = self:_get_condition_token(cond, op, dval)
  return self:_handle_where_token(where_token, "%s OR %s")
end

---@param self Sql
---@param cond table|string|function
---@param op? string
---@param dval? DBValue
---@return Sql
function Sql.or_where_not(self, cond, op, dval)
  local where_token = self:_get_condition_token_not(cond, op, dval)
  return self:_handle_where_token(where_token, "%s OR %s")
end

---@param self Sql
---@param builder Sql|string
---@return Sql
function Sql.where_exists(self, builder)
  if self._where then
    self._where = string_format("(%s) AND EXISTS (%s)", self._where, builder)
  else
    self._where = string_format("EXISTS (%s)", builder)
  end
  return self
end

---@param self Sql
---@param builder Sql|string
---@return Sql
function Sql.where_not_exists(self, builder)
  if self._where then
    self._where = string_format("(%s) AND NOT EXISTS (%s)", self._where, builder)
  else
    self._where = string_format("NOT EXISTS (%s)", builder)
  end
  return self
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql.where_in(self, cols, range)
  if type(cols) == "string" then
    return Sql._base_where_in(self, self:_get_column(cols), range)
  else
    local res = {}
    for i = 1, #cols do
      res[i] = self:_get_column(cols[i])
    end
    return Sql._base_where_in(self, res, range)
  end
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql.where_not_in(self, cols, range)
  if type(cols) == "string" then
    cols = self:_get_column(cols)
  else
    for i = 1, #cols do
      cols[i] = self:_get_column(cols[i])
    end
  end
  return Sql._base_where_not_in(self, cols, range)
end

---@param self Sql
---@param col string
---@return Sql
function Sql.where_null(self, col)
  return Sql._base_where_null(self, self:_get_column(col))
end

---@param self Sql
---@param col string
---@return Sql
function Sql.where_not_null(self, col)
  return Sql._base_where_not_null(self, self:_get_column(col))
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql.where_between(self, col, low, high)
  return Sql._base_where_between(self, self:_get_column(col), low, high)
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql.where_not_between(self, col, low, high)
  return Sql._base_where_not_between(self, self:_get_column(col), low, high)
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql.or_where_in(self, cols, range)
  if type(cols) == "string" then
    cols = self:_get_column(cols)
  else
    for i = 1, #cols do
      cols[i] = self:_get_column(cols[i])
    end
  end
  return Sql._base_or_where_in(self, cols, range)
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql.or_where_not_in(self, cols, range)
  if type(cols) == "string" then
    cols = self:_get_column(cols)
  else
    for i = 1, #cols do
      cols[i] = self:_get_column(cols[i])
    end
  end
  return Sql._base_or_where_not_in(self, cols, range)
end

---@param self Sql
---@param col string
---@return Sql
function Sql.or_where_null(self, col)
  return Sql._base_or_where_null(self, self:_get_column(col))
end

---@param self Sql
---@param col string
---@return Sql
function Sql.or_where_not_null(self, col)
  return Sql._base_or_where_not_null(self, self:_get_column(col))
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql.or_where_between(self, col, low, high)
  return Sql._base_or_where_between(self, self:_get_column(col), low, high)
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql.or_where_not_between(self, col, low, high)
  return Sql._base_or_where_not_between(self, self:_get_column(col), low, high)
end

---@param self Sql
---@param builder Sql
---@return Sql
function Sql.or_where_exists(self, builder)
  if self._where then
    self._where = string_format("%s OR EXISTS (%s)", self._where, builder)
  else
    self._where = string_format("EXISTS (%s)", builder)
  end
  return self
end

---@param self Sql
---@param builder Sql
---@return Sql
function Sql.or_where_not_exists(self, builder)
  if self._where then
    self._where = string_format("%s OR NOT EXISTS (%s)", self._where, builder)
  else
    self._where = string_format("NOT EXISTS (%s)", builder)
  end
  return self
end

---@param self Sql
---@param cond table|string|function
---@param op? DBValue
---@param dval? DBValue
function Sql.having(self, cond, op, dval)
  if self._having then
    self._having = string_format("(%s) AND (%s)", self._having, self:_get_condition_token(cond, op, dval))
  else
    self._having = self:_get_condition_token(cond, op, dval)
  end
  return self
end

---@param self Sql
---@param cond table|string|function
---@param op? DBValue
---@param dval? DBValue
function Sql.having_not(self, cond, op, dval)
  if self._having then
    self._having = string_format("(%s) AND (%s)", self._having, self:_get_condition_token_not(cond, op, dval))
  else
    self._having = self:_get_condition_token_not(cond, op, dval)
  end
  return self
end

---@param self Sql
---@param builder Sql
---@return Sql
function Sql.having_exists(self, builder)
  if self._having then
    self._having = string_format("(%s) AND EXISTS (%s)", self._having, builder)
  else
    self._having = string_format("EXISTS (%s)", builder)
  end
  return self
end

---@param self Sql
---@param builder Sql
---@return Sql
function Sql.having_not_exists(self, builder)
  if self._having then
    self._having = string_format("(%s) AND NOT EXISTS (%s)", self._having, builder)
  else
    self._having = string_format("NOT EXISTS (%s)", builder)
  end
  return self
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql.having_in(self, cols, range)
  local in_token = self:_get_in_token(cols, range)
  if self._having then
    self._having = string_format("(%s) AND %s", self._having, in_token)
  else
    self._having = in_token
  end
  return self
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql.having_not_in(self, cols, range)
  local not_in_token = self:_get_in_token(cols, range, "NOT IN")
  if self._having then
    self._having = string_format("(%s) AND %s", self._having, not_in_token)
  else
    self._having = not_in_token
  end
  return self
end

---@param self Sql
---@param col string
---@return Sql
function Sql.having_null(self, col)
  if self._having then
    self._having = string_format("(%s) AND %s IS NULL", self._having, col)
  else
    self._having = col .. " IS NULL"
  end
  return self
end

---@param self Sql
---@param col string
---@return Sql
function Sql.having_not_null(self, col)
  if self._having then
    self._having = string_format("(%s) AND %s IS NOT NULL", self._having, col)
  else
    self._having = col .. " IS NOT NULL"
  end
  return self
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql.having_between(self, col, low, high)
  if self._having then
    self._having = string_format("(%s) AND (%s BETWEEN %s AND %s)", self._having, col, low, high)
  else
    self._having = string_format("%s BETWEEN %s AND %s", col, low, high)
  end
  return self
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql.having_not_between(self, col, low, high)
  if self._having then
    self._having = string_format("(%s) AND (%s NOT BETWEEN %s AND %s)", self._having, col, low, high)
  else
    self._having = string_format("%s NOT BETWEEN %s AND %s", col, low, high)
  end
  return self
end

---@param self Sql
---@param cond table|string|function
---@param op? DBValue
---@param dval? DBValue
function Sql.or_having(self, cond, op, dval)
  if self._having then
    self._having = string_format("%s OR %s", self._having, self:_get_condition_token(cond, op, dval))
  else
    self._having = self:_get_condition_token(cond, op, dval)
  end
  return self
end

---@param self Sql
---@param cond table|string|function
---@param op? DBValue
---@param dval? DBValue
function Sql.or_having_not(self, cond, op, dval)
  if self._having then
    self._having = string_format("%s OR %s", self._having, self:_get_condition_token_not(cond, op, dval))
  else
    self._having = self:_get_condition_token_not(cond, op, dval)
  end
  return self
end

---@param self Sql
---@param builder Sql
---@return Sql
function Sql.or_having_exists(self, builder)
  if self._having then
    self._having = string_format("%s OR EXISTS (%s)", self._having, builder)
  else
    self._having = string_format("EXISTS (%s)", builder)
  end
  return self
end

---@param self Sql
---@param builder Sql
---@return Sql
function Sql.or_having_not_exists(self, builder)
  if self._having then
    self._having = string_format("%s OR NOT EXISTS (%s)", self._having, builder)
  else
    self._having = string_format("NOT EXISTS (%s)", builder)
  end
  return self
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql.or_having_in(self, cols, range)
  local in_token = self:_get_in_token(cols, range)
  if self._having then
    self._having = string_format("%s OR %s", self._having, in_token)
  else
    self._having = in_token
  end
  return self
end

---@param self Sql
---@param cols string|string[]
---@param range Sql|table|string
---@return Sql
function Sql.or_having_not_in(self, cols, range)
  local not_in_token = self:_get_in_token(cols, range, "NOT IN")
  if self._having then
    self._having = string_format("%s OR %s", self._having, not_in_token)
  else
    self._having = not_in_token
  end
  return self
end

---@param self Sql
---@param col string
---@return Sql
function Sql.or_having_null(self, col)
  if self._having then
    self._having = string_format("%s OR %s IS NULL", self._having, col)
  else
    self._having = col .. " IS NULL"
  end
  return self
end

---@param self Sql
---@param col string
---@return Sql
function Sql.or_having_not_null(self, col)
  if self._having then
    self._having = string_format("%s OR %s IS NOT NULL", self._having, col)
  else
    self._having = col .. " IS NOT NULL"
  end
  return self
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql.or_having_between(self, col, low, high)
  if self._having then
    self._having = string_format("%s OR (%s BETWEEN %s AND %s)", self._having, col, low, high)
  else
    self._having = string_format("%s BETWEEN %s AND %s", col, low, high)
  end
  return self
end

---@param self Sql
---@param col string
---@param low number
---@param high number
---@return Sql
function Sql.or_having_not_between(self, col, low, high)
  if self._having then
    self._having = string_format("%s OR (%s NOT BETWEEN %s AND %s)", self._having, col, low, high)
  else
    self._having = string_format("%s NOT BETWEEN %s AND %s", col, low, high)
  end
  return self
end

---@param self Sql
---@param a DBValue
---@param b? DBValue
---@param ...? DBValue
---@return Sql
function Sql.distinct_on(self, a, b, ...)
  local s = self:_get_select_token(a, b, ...)
  self._distinct_on = s
  self._order = s
  return self
end

---@param self Sql
---@param name string
---@param amount? number
---@return Sql
function Sql.increase(self, name, amount)
  return self:update { [name] = self.token(string_format("%s + %s", name, amount or 1)) }
end

---@param self Sql
---@param name string
---@param amount? number
---@return Sql
function Sql.decrease(self, name, amount)
  return self:update { [name] = self.token(string_format("%s - %s", name, amount or 1)) }
end

--- {{id=1}, {id=2}, {id=3}} => columns: {'id'}  keys: {{1},{2},{3}}
--- each row of keys must be the same struct, so get columns from first row
---@param self Sql
---@param keys Record[]
---@param columns? string[]
---@return Sql
function Sql._base_get_multiple(self, keys, columns)
  if #keys == 0 then
    error("empty keys passed to get_multiple")
  end
  columns = columns or get_keys(keys[1])
  keys, columns = self:_get_cte_values_literal(keys, columns, false)
  local join_cond = self:_get_join_conditions(columns, "V", self._as or self.table_name)
  local cte_name = string_format("V(%s)", table_concat(columns, ", "))
  local cte_values = string_format("(VALUES %s)", as_token(keys))
  return self:with(cte_name, cte_values):right_join("V", join_cond)
end

---@param self Sql
---@param rows Record[]
---@param columns? string[]
---@param no_check? boolean
---@return string[], string[]
function Sql._get_cte_values_literal(self, rows, columns, no_check)
  columns = columns or get_keys(rows)
  rows = self:_rows_to_array(rows, columns)
  local first_row = rows[1]
  for i, col in ipairs(columns) do
    local field = self:_find_field_model(col)
    if field then
      first_row[i] = string_format("%s::%s", as_literal(first_row[i]), field.db_type)
    elseif no_check then
      first_row[i] = as_literal(first_row[i])
    else
      error("invalid field name for _get_cte_values_literal: " .. col)
    end
  end
  ---@type string[]
  local res = {}
  res[1] = '(' .. as_token(first_row) .. ')'
  for i = 2, #rows, 1 do
    res[i] = as_literal(rows[i])
  end
  return res, columns
end

---@param self Sql
---@param join_args table
---@param join_type? JOIN_TYPE
---@return table, boolean
function Sql._register_join_model(self, join_args, join_type)
  join_type = join_type or join_args.join_type or "INNER"
  local find = true
  -- 有可能出现self.model.table_name和self.table_name不一样的情况,例如get_or_create
  local model, table_name
  if join_args.model then
    model = join_args.model
    table_name = model.table_name
  else
    model = self.model
    table_name = self.table_name
  end
  if not self._join_keys then
    self._join_keys = {}
  end
  if join_args.join_callback then
    local fk_model = join_args.fk_model
    local t = self._as or self.table_name
    local fk_alias = 'T' .. self:_get_join_number()
    local join_cond = join_args.join_callback(t, fk_alias)
    local join_key = fk_alias
    local join_obj = {
      join_type = join_type,
      model = model,
      alias = t,
      fk_model = fk_model,
      fk_alias = fk_alias
    }
    local join_table = string_format("%s %s", fk_model.table_name, join_obj.fk_alias)
    self:_handle_join(join_type, join_table, join_cond)
    self._join_keys[join_key] = join_obj
    if join_args.sql_callback then
      join_args.sql_callback(fk_alias)
    end
    return join_obj, false
  else
    local column = join_args.column
    local f = assert(model.fields[column], string_format("invalid name %s for model %s", column, table_name))
    local fk_model = join_args.fk_model or f and f.reference
    local fk_column = join_args.fk_column or f and f.reference_column
    local join_key
    if join_args.join_key == nil then
      if self.table_name == table_name then
        -- 如果是本体model连接, 则join_key的定义方式与_parse_column和load_fk一致,避免生成重复的join
        -- 同一个列,可能和不同的表join多次, 因此要加上外键表名, 避免自动判断错误
        join_key = column .. "__" .. fk_model.table_name
      else
        join_key = string_format("%s__%s__%s__%s__%s",
          join_type, table_name, column, fk_model.table_name, fk_column)
      end
    else
      join_key = join_args.join_key
    end
    local join_obj = self._join_keys[join_key]
    if not join_obj then
      find = false
      join_obj = {
        join_type = join_type,
        model = model,
        column = column,
        alias = join_args.alias or table_name,
        fk_model = fk_model,
        fk_column = fk_column,
        fk_alias = join_args.fk_alias or ('T' .. self:_get_join_number())
      }
      local join_table = string_format("%s %s", fk_model.table_name, join_obj.fk_alias)
      local join_cond = string_format("%s.%s = %s.%s", join_obj.alias, join_obj.column, join_obj.fk_alias,
        join_obj.fk_column)
      self:_handle_join(join_type, join_table, join_cond)
      self._join_keys[join_key] = join_obj
    end
    return join_obj, find
  end
end

---@param self Sql
---@param col string
---@return Field?, Xodel?,string?
function Sql._find_field_model(self, col)
  local field = self.model.fields[col]
  if field then
    return field, self.model, self._as or self.table_name
  end
  if not self._join_keys then
    return
  end
  for _, join_obj in pairs(self._join_keys) do
    local fk_field = join_obj.fk_model.fields[col]
    -- searching in join args where depth = 1
    if fk_field and join_obj.model.table_name == self.table_name then
      return fk_field, join_obj.fk_model, (join_obj.fk_alias or join_obj.fk_model.table_name)
    end
  end
end

function Sql._parse_column(self, key, as_select, disable_alias)
  --TODO: support json field searching like django:
  -- https://docs.djangoproject.com/en/4.2/topics/db/queries/#querying-jsonfield
  -- https://www.postgresql.org/docs/current/functions-json.html
  local a, b = key:find("__", 1, true)
  if not a then
    if as_select then
      return self:_get_column(key)
    else
      return self:_get_column(key), "eq", key
    end
  end
  local token = key:sub(1, a - 1)
  --TODO: 这里应该使用_find_field_model来涵盖key为已登记的fk_model的列的情况吗?
  -- 例如 table usr(email), table profile(usr_id reference usr),
  -- => profile:load_fk('usr_id'):where{email__startswith}
  local field, model, prefix = self:_find_field_model(token)
  if not field then
    error(string_format("%s is not a valid field name for %s", token, self.table_name))
  end
  local i, fk_model, rc, join_key, op
  local field_name = token
  while true do
    -- get next token seprated by __
    i = b + 1
    a, b = key:find("__", i, true)
    if not a then
      token = key:sub(i)
    else
      token = key:sub(i, a - 1)
    end
    if field.reference then
      fk_model = field.reference
      rc = field.reference_column
      local fk_model_field = fk_model.fields[token]
      if not fk_model_field then
        -- fk__eq, compare on fk value directly
        op = token
        break
      elseif token == field.reference_column then
        -- fk__id, unnecessary suffix, ignore
        break
      else
        -- fk__name, need inner join
        if not join_key then
          -- prefix with field_name because fk_model can be referenced multiple times
          join_key = field_name -- .. "__" .. fk_model.table_name --2024.1.21:没必要加入table_name
        else
          join_key = join_key .. "__" .. field_name
        end
        local join_obj = self:_register_join_model {
          join_type = self._join_type or "INNER",
          join_key = join_key,
          model = model,
          column = field_name,
          -- alias = prefix or model.table_name, -- prefix永不为空, 这句不知怎么来的
          alias = assert(prefix, "prefix in _parse_column should never be falsy"),
          fk_model = fk_model,
          fk_column = rc
        }
        -- set state recursively
        field = fk_model_field
        model = fk_model --[[@as Xodel]]
        prefix = join_obj.fk_alias
        field_name = token
      end
    elseif field.model then
      -- jsonb field: persons__sfzh='xxx' => persons @> '[{"sfzh":"xxx"}]'
      local table_field = field.model.fields[token]
      if not table_field then
        error(string_format("invalid table field name %s of %s", token, field.name))
      end
      op = function(value)
        if type(value) == 'string' and value:find("'", 1, true) then
          value = value:gsub("'", "''")
        end
        return string_format([[@> '[{"%s":%s}]']], token, encode(value))
      end
      break
    else
      -- non_fk__lt, non_fk__gt, etc
      op = token
      break
    end
    if not a then
      break
    end
  end
  local final_key = prefix .. "." .. field_name
  if as_select and not disable_alias then
    -- ensure select("fk__name") will return { fk__name= 'foo'}
    -- in case of error like Profile:select('usr_id__eq')
    assert(fk_model, string_format("should contains foreignkey field name: %s", key))
    assert(op == nil, string_format("invalid field name: %s", op))
    return final_key .. ' AS ' .. key
  else
    --TODO: 为什么这里返回第三个参数field_name?
    return final_key, op or 'eq', field_name
  end
end

---@param self Sql
---@param key string
---@return string
function Sql._get_column(self, key)
  if self.model.fields[key] then
    if self._as then
      return self._as .. '.' .. key
    else
      return self.model.name_cache[key]
    end
  end
  if key == '*' then
    return '*'
  end
  local table_name, column = match(key, '^([\\w_]+)[.]([\\w_]+)$', 'josui')
  if table_name then
    return key
  end
  if self._join_keys then
    for _, join_obj in pairs(self._join_keys) do
      if join_obj.model.table_name == self.table_name and join_obj.fk_model.fields[key] then
        return join_obj.fk_alias .. '.' .. key
      end
    end
  end
  error(string_format("invalid field name: '%s'", key))
end

local string_db_types = {
  varchar = true,
  text = true,
  char = true,
  bpchar = true
}
local string_operators = {
  contains = true,
  startswith = true,
  endswith = true,
  regex = true,
  regex_insensitive = true,
  regex_sensitive = true,
}
---@param self Sql
---@param value DBValue
---@param key string
---@param op string
---@param raw_key string
---@return string
function Sql._get_expr_token(self, value, key, op, raw_key)
  local field = self.model.fields[raw_key]
  if field and not string_db_types[field.db_type] and string_operators[op] then
    key = key .. '::varchar'
  end
  if op == "eq" then
    return string_format("%s = %s", key, as_literal(value))
  elseif op == "in" then
    return string_format("%s IN %s", key, as_literal(value))
  elseif op == "notin" then
    return string_format("%s NOT IN %s", key, as_literal(value))
  elseif COMPARE_OPERATORS[op] then
    return string_format("%s %s %s", key, COMPARE_OPERATORS[op], as_literal(value))
  elseif op == "contains" then
    ---@cast value string
    return string_format("%s LIKE '%%%s%%'", key, value:gsub("'", "''"))
  elseif op == "startswith" then
    ---@cast value string
    return string_format("%s LIKE '%s%%'", key, value:gsub("'", "''"))
  elseif op == "endswith" then
    ---@cast value string
    return string_format("%s LIKE '%%%s'", key, value:gsub("'", "''"))
  elseif op == "regex" or op == "regex_sensitive" then
    ---@cast value string
    return string_format("%s ~ '%%%s'", key, value:gsub("'", "''"))
  elseif op == "regex_insensitive" then
    ---@cast value string
    return string_format("%s ~* '%%%s'", key, value:gsub("'", "''"))
  elseif op == "null" then
    if value then
      return string_format("%s IS NULL", key)
    else
      return string_format("%s IS NOT NULL", key)
    end
  elseif type(op) == 'function' then
    return string_format("%s %s", key, op(value))
  else
    error("invalid sql op: " .. tostring(op))
  end
end

---@param self Sql
---@param rows Records|Sql
---@param columns? string[]
---@return Sql
function Sql.insert(self, rows, columns)
  if not is_sql_instance(rows) then
    ---@cast rows Records
    if not self._skip_validate then
      ---@diagnostic disable-next-line: cast-local-type
      rows, columns = self.model:validate_create_data(rows, columns)
      if rows == nil then
        error(columns)
      end
    end
    ---@diagnostic disable-next-line: cast-local-type, param-type-mismatch
    rows, columns = self.model:prepare_db_rows(rows, columns)
    if rows == nil then
      error(columns)
    end
    ---@diagnostic disable-next-line: param-type-mismatch
    return Sql._base_insert(self, rows, columns)
  else
    ---@cast rows Sql
    return Sql._base_insert(self, rows, columns)
  end
end

---@param self Sql
---@param row Record|Sql|string
---@param columns? string[]
---@return Sql
function Sql.update(self, row, columns)
  if type(row) == 'string' then
    return Sql._base_update(self, row)
  elseif not is_sql_instance(row) then
    local err
    ---@cast row Record
    if not self._skip_validate then
      ---@diagnostic disable-next-line: cast-local-type
      row, err = self.model:validate_update(row, columns)
      if row == nil then
        error(err)
      end
    end
    ---@diagnostic disable-next-line: cast-local-type
    row, columns = self.model:prepare_db_rows(row, columns, true)
    if row == nil then
      error(columns)
    end
    ---@diagnostic disable-next-line: param-type-mismatch
    return Sql._base_update(self, row, columns)
  else
    ---@cast row Sql
    return Sql._base_update(self, row, columns)
  end
end

function Sql._get_bulk_key(self, columns)
  if self.model.unique_together and self.model.unique_together[1] then
    return self.model.unique_together[1]
  end
  for index, name in ipairs(columns) do
    local f = self.model.fields[name]
    if f and f.unique then
      return name
    end
  end
  local pk = self.model.primary_key
  if pk and Array.includes(columns, pk) then
    return pk
  end
  return pk
end

function Sql._clean_bulk_params(self, rows, key, columns)
  if isempty(rows) then
    error("empty rows passed to merge")
  end
  if not rows[1] then
    rows = { rows }
  end
  if columns == nil then
    columns = {}
    for k, _ in pairs(rows[1]) do
      if self.model.fields[k] then
        columns[#columns + 1] = k
      end
    end
    if #columns == 0 then
      error("no columns provided for bulk")
    end
  end
  if key == nil then
    key = self:_get_bulk_key(columns)
  end
  if type(key) == 'string' then
    if not Array.includes(columns, key) then
      columns = { key, unpack(columns) }
    end
  elseif type(key) == 'table' then
    for _, k in ipairs(key) do
      if not Array.includes(columns, k) then
        columns = { k, unpack(columns) }
      end
    end
  else
    error("invalid key type for bulk:" .. type(key))
  end
  return rows, key, columns
end

---@param self Sql
---@param rows Record[]
---@param key? Keys
---@param columns? string[]
---@return Sql|XodelInstance[]
function Sql.merge(self, rows, key, columns)
  rows, key, columns = self:_clean_bulk_params(rows, key, columns)
  if not self._skip_validate then
    ---@diagnostic disable-next-line: cast-local-type
    rows, key, columns = self.model:validate_create_rows(rows, key, columns)
    if rows == nil then
      error(key)
    end
  end
  ---@diagnostic disable-next-line: cast-local-type, param-type-mismatch
  rows, columns = self.model:prepare_db_rows(rows, columns, false)
  if rows == nil then
    error(columns)
  end
  ---@diagnostic disable-next-line: param-type-mismatch
  return Sql._base_merge(self, rows, key, columns)
end

---@param self Sql
---@param rows Record[]
---@param key? Keys
---@param columns? string[]
---@return Sql|XodelInstance[]
function Sql.upsert(self, rows, key, columns)
  rows, key, columns = self:_clean_bulk_params(rows, key, columns)
  if not self._skip_validate then
    ---@diagnostic disable-next-line: cast-local-type
    rows, key, columns = self.model:validate_create_rows(rows, key, columns)
    if rows == nil then
      error(key)
    end
  end
  ---@diagnostic disable-next-line: cast-local-type, param-type-mismatch
  rows, columns = self.model:prepare_db_rows(rows, columns, false)
  if rows == nil then
    error(columns)
  end
  ---@diagnostic disable-next-line: param-type-mismatch
  return Sql._base_upsert(self, rows, key, columns)
end

---@param self Sql
---@param rows Record[]
---@param key? Keys
---@param columns? string[]
---@return Sql|XodelInstance[]
function Sql.updates(self, rows, key, columns)
  rows, key, columns = self:_clean_bulk_params(rows, key, columns)
  if not self._skip_validate then
    ---@diagnostic disable-next-line: cast-local-type
    rows, key, columns = self.model:validate_update_rows(rows, key, columns)
    if rows == nil then
      error(columns)
    end
  end
  ---@diagnostic disable-next-line: cast-local-type, param-type-mismatch
  rows, columns = self.model:prepare_db_rows(rows, columns, true)
  if rows == nil then
    error(columns)
  end
  ---@diagnostic disable-next-line: param-type-mismatch
  return Sql._base_updates(self, rows, key, columns)
end

---@param self Sql
---@param keys Record[]
---@param columns string[]
---@return Sql|XodelInstance[]
function Sql.get_multiple(self, keys, columns)
  if self._commit == nil or self._commit then
    return Sql._base_get_multiple(self, keys, columns):exec()
  else
    return Sql._base_get_multiple(self, keys, columns)
  end
end

---@param self Sql
---@param statement string
---@return array<XodelInstance>, table?
function Sql.exec_statement(self, statement)
  local records = assert(self.model.query(statement, self._compact))
  local multiple_records
  if self._prepend then
    multiple_records = records
    records = records[#self._prepend + 1]
  elseif self._append then
    multiple_records = records
    records = records[1]
  end
  if (self._raw or self._raw == nil) or self._compact or self._update or self._insert or self._delete then
    if (self._update or self._insert or self._delete) and self._returning then
      records.affected_rows = nil
    end
    ---@cast records array<Record>
    return setmetatable(records, Array), multiple_records
  else
    ---@type Xodel
    local cls = self.model
    if not self._load_fk then
      for i, record in ipairs(records) do
        records[i] = cls:load(record)
      end
    else
      ---@type {[string]:Field}
      local fields = cls.fields
      local field_names = cls.field_names
      for i, record in ipairs(records) do
        for _, name in ipairs(field_names) do
          local field = fields[name]
          local value = record[name]
          if value ~= nil then
            local fk_model = self._load_fk[name]
            if not fk_model then
              if not field.load then
                record[name] = value
              else
                record[name] = field:load(value)
              end
            else
              -- _load_fk means reading attributes of a foreignkey,
              -- so the on-demand reading mode of foreignkey_db_to_lua_validator is not proper here
              record[name] = fk_model:load(get_foreign_object(record, name .. "__"))
            end
          end
        end
        records[i] = cls:create_record(record)
      end
    end
    ---@cast records array<XodelInstance>
    return setmetatable(records, Array), multiple_records
  end
end

---@param self Sql
---@return array<XodelInstance>, table?
function Sql.exec(self)
  return self:exec_statement(self:statement())
end

---@param self Sql
---@param cond? table|string|function
---@param op? string
---@param dval? DBValue
---@return integer
function Sql.count(self, cond, op, dval)
  local res, err
  if cond ~= nil then
    res = self:_base_select("count(*)"):where(cond, op, dval):compact():exec()
  else
    res = self:_base_select("count(*)"):compact():exec()
  end
  return res[1][1]
end

---@param self Sql
---@param rows Records|Sql
---@param columns? string[]
---@return Sql
function Sql.create(self, rows, columns)
  return self:insert(rows, columns):execr()
end

---@param self Sql
---@return boolean
function Sql.exists(self)
  local statement = string_format("SELECT EXISTS (%s)", self:select(1):limit(1):compact():statement())
  local res, err = self.model.query(statement, self._compact)
  if res == nil then
    error(err)
  else
    return res[1][1]
  end
end

---@param self Sql
---@return Sql
function Sql.compact(self)
  self._compact = true
  return self
end

---@param self Sql
---@param is_raw? boolean
---@return Sql
function Sql.raw(self, is_raw)
  if is_raw == nil or is_raw then
    self._raw = true
  else
    self._raw = false
  end
  return self
end

---@param self Sql
---@param bool boolean
---@return Sql
function Sql.commit(self, bool)
  if bool == nil then
    bool = true
  end
  self._commit = bool
  return self
end

---@param self Sql
---@param jtype string
---@return Sql
function Sql.join_type(self, jtype)
  self._join_type = jtype
  return self
end

---@param self Sql
---@param bool? boolean
---@return Sql
function Sql.skip_validate(self, bool)
  if bool == nil then
    bool = true
  end
  self._skip_validate = bool
  return self
end

---@param self Sql
---@param col? string
---@return Record[]
function Sql.flat(self, col)
  if col then
    if self._update or self._delete or self._insert then
      return self:returning(col):compact():execr():flat()
    else
      return self:select(col):compact():execr():flat()
    end
  else
    return self:compact():execr():flat()
  end
end

---@param self Sql
---@param cond? table|string|function
---@param op? string
---@param dval? DBValue
---@return XodelInstance?, number?
function Sql.try_get(self, cond, op, dval)
  if self._raw == nil then
    self._raw = false
  end
  local records
  if cond ~= nil then
    if type(cond) == 'table' and next(cond) == nil then
      error("empty condition table is not allowed")
    end
    records = self:where(cond, op, dval):limit(2):exec()
  else
    records = self:limit(2):exec()
  end
  if #records == 1 then
    return records[1]
  else
    return nil, #records
  end
end

---@param self Sql
---@param cond? table|string|function
---@param op? string
---@param dval? DBValue
---@return XodelInstance
function Sql.get(self, cond, op, dval)
  local record, record_number = self:try_get(cond, op, dval)
  if not record then
    if record_number == 0 then
      error("record not found")
    else
      error("multiple records returned: " .. record_number)
    end
  else
    return record
  end
end

---@param self Sql
---@return Set
function Sql.as_set(self)
  return self:compact():execr():flat():as_set()
end

---@param self Sql
---@return table|array<Record>
function Sql.execr(self)
  return self:raw():exec()
end

---@param self Sql
---@return Sql
function Sql.load_all_fk_labels(self)
  for i, name in ipairs(self.model.names) do
    local field = self.model.fields[name]
    if field and field.type == 'foreignkey' and field.reference_label_column ~= field.reference_column then
      self:load_fk(field.name, field.reference_label_column)
    end
  end
  return self
end

---@param self Sql
---@param fk_name string
---@param select_names string[]|string
---@param ... string
---@return Sql
function Sql.load_fk(self, fk_name, select_names, ...)
  -- psr:load_fk('parent_id', '*')
  -- psr:load_fk('parent_id', 'usr_id')
  -- psr:load_fk('parent_id', {'usr_id'})
  -- psr:load_fk('parent_id', 'usr_id__xm')
  local fk = self.model.foreign_keys[fk_name]
  if fk == nil then
    error(fk_name .. " is not a valid forein key name for " .. self.table_name)
  end
  local fk_model = fk.reference
  if not self._load_fk then
    self._load_fk = {}
  end
  self._load_fk[fk_name] = fk_model
  self:select(fk_name)
  if not select_names then
    return self
  end
  local fks = {}
  if type(select_names) == 'table' then
    for _, fkn in ipairs(select_names) do
      fks[#fks + 1] = string_format("%s__%s", fk_name, fkn)
    end
  elseif select_names == '*' then
    for i, fkn in ipairs(fk_model.field_names) do
      fks[#fks + 1] = string_format("%s__%s", fk_name, fkn)
    end
  elseif type(select_names) == 'string' then
    fks[#fks + 1] = string_format("%s__%s", fk_name, select_names)
    for i = 1, select("#", ...) do
      local fkn = select(i, ...)
      fks[#fks + 1] = string_format("%s__%s", fk_name, fkn)
    end
  else
    error(string_format("invalid argument type %s for load_fk", type(select_names)))
  end
  return self:select(fks)
end

-- WITH RECURSIVE
--   branch_recursive AS (
--     SELECT
--       branch.id,
--       branch.name,
--       branch.pid
--     FROM
--       branch
--     WHERE
--       branch.pid = 1
--     UNION ALL
--     (
--       SELECT
--         branch.id,
--         branch.name,
--         branch.pid
--       FROM
--         branch
--         INNER JOIN branch_recursive ON (branch.pid = branch_recursive.id)
--     )
--   )
-- SELECT
--   branch.id,
--   branch.name,
--   branch.pid
-- FROM
--   branch_recursive AS branch;

---@param self Sql
---@param name string
---@param value any
---@param select_names? string[]
---@return Sql
function Sql.where_recursive(self, name, value, select_names)
  local fk = self.model.foreign_keys[name]
  if fk == nil then
    error(name .. " is not a valid forein key name for " .. self.table_name)
  end
  local fkc = fk.reference_column
  local tname = self.model.table_name
  local t_alias = tname .. '_recursive'
  local seed_sql = self.model:create_sql():select(fkc, name):where(name, value)
  local join_cond = string_format("%s.%s = %s.%s", tname, name, t_alias, fkc)
  local recursive_sql = self.model:create_sql():select(fkc, name):_base_join('INNER', t_alias, join_cond)
  if select_names then
    seed_sql:select(select_names)
    recursive_sql:select(select_names)
  end
  self:with_recursive(t_alias, seed_sql:union_all(recursive_sql))
  return self:from(t_alias .. ' AS ' .. tname)
end

return Sql
