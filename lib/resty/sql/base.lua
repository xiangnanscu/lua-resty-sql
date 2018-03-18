local ngx_re_split = require "ngx.re".split
local ngx_re_match = ngx.re.match
local rawget = rawget
local setmetatable = setmetatable
local ipairs = ipairs
local tostring = tostring
local type = type
local pairs = pairs
local string_format = string.format
local table_concat = table.concat
local table_insert = table.insert

-- todo: F object like Django
-- NOTE, you'd better not name your foreign key like T1, T2, T10 etc
-- there should be no '__' in your table name or field name

local CHAIN_METHODS = {
    "select", 
    "where", 
    "group", 
    "having", 
    "order", 
    "limit", 
    "offset", 
    "join", 
    "create", 
    "update", 
    "delete",
    "distinct",
}
-- todo: case sensitive
local RELATIONS = {
    lt='%s < %s', 
    lte='%s <= %s', 
    gt='%s > %s', 
    gte='%s >= %s', 
    ne='%s <> %s', 
    eq='%s = %s', 
    ['in']='%s IN %s',
}
local LIKE_RELATIONS = {
    contains = "%s LIKE '%%%s%%'",
    startswith = "%s LIKE '%s%%'",
    endswith = "%s LIKE '%%%s'",
}

local function map(tbl, func)
    local res = {}
    for i=1, #tbl do
        res[i] = func(tbl[i])
    end
    return res
end

local Q = {negated='', op=''}
-- 3 operators to control logic: - , / and *
-- there's no stackoverflow as long as you don't access non-existed property
setmetatable(Q, Q) 
Q.__index = Q
function Q.__unm(t) 
    if t.negated == '' then
        t.negated = 'NOT'
    else
        t.negated = ''
    end
    return t
end
function Q.__mul(t, o) --and
    local n = setmetatable({}, Q)
    n.left = t
    n.right = o
    n.op = 'AND'
    return n
end
function Q.__div(t, o) --or
    local n = setmetatable({}, Q)
    n.left = t
    n.right = o
    n.op = 'OR'
    return n
end
function Q.__call(t, kwargs) 
    return setmetatable(kwargs, Q)
end
function Q.serialize(self, sql)
    if self.op == '' then
        local pair, err = sql:_parse_params(nil, self)
        if err then
            return nil, err
        end
        return string_format('%s(%s)', self.negated, pair)
    else -- AND or OR
        local left_pair, err = self.left:serialize(sql)
        if err then
            return nil, err
        end
        local right_pair, err = self.right:serialize(sql)
        if err then
            return nil, err
        end
        return string_format('%s(%s %s %s)', self.negated, left_pair, self.op, right_pair)
    end
end

local Sql = {}
Sql.Q = Q
Sql.__index = Sql
function Sql.new(cls, self)
    return setmetatable(self or {}, cls)
end
function Sql.bind_model(cls, model)
    assert(model, "you must provide a model")
    cls.model = model
    local t = {}
    cls._quoted_table_name = cls.escape_name(model.table_name)
    cls._quoted_table_name_prefix = cls._quoted_table_name..'.'
    for i, field in ipairs(model.fields) do
        table_insert(t, cls._quoted_table_name_prefix..cls.escape_name(field.name))
    end
    cls._select_all_string_cache = table_concat(t, ', ')    
    return cls
end
function Sql.class(cls, subcls)
    subcls = subcls or {}
    -- copy attrs for faster lookup
    for k, v in pairs(cls) do 
        if rawget(subcls, k) == nil then
            subcls[k] = v
        end
    end
    subcls.__index = subcls
    return setmetatable(subcls, cls)
end
function Sql.join(self, params)
    -- ** need test
    if self._join == nil then
        self._join = {} 
    end
    if self._select_join == nil then
        self._select_join = {}
    end
    local model = self.model
    for i, right_table_alias in ipairs(params) do
        -- order matters,  so used as a array
        local fk_model = model.foreign_keys[right_table_alias].reference
        self:_add_to_join{
            left_table = model.table_name,  -- record
            right_table = fk_model.table_name, -- user
            right_table_alias = right_table_alias, -- buyer
            left_field = right_table_alias,
            key = '__'..right_table_alias,
        } -- user
        self._select_join[right_table_alias] = fk_model
    end
    return self
end
function Sql.statement(self)
    if self._update then
        return self:to_update_sql()
    elseif self._create then
        -- this is always a single table operation
        local names = {}
        local values = {}
        local escn = self.escape_name
        local escv = self.escape_value
        for k, v in pairs(self._create) do
            names[#names+1] = escn(k)
            values[#values+1] = escv(v)
        end
        return string_format('INSERT INTO %s (%s) VALUES (%s)', self._quoted_table_name, table_concat(names,', '), table_concat(values,', '))
    elseif self._delete then 
        -- this is always a single table operation
        return string_format('DELETE FROM %s%s', self._quoted_table_name, self:_parse_where())
    --SELECT..FROM..WHERE..GROUP BY..HAVING..ORDER BY
    else 
        self._is_select = true --for fast judgement in the `exec` method
        local where_clause, err = self:_parse_where()
        if err then 
            return nil, err
        end
        return string_format('SELECT %s%s FROM %s %s%s%s%s%s%s', 
            self._distinct and 'DISTINCT' or '',
            self:_parse_select(), 
            self:_parse_from(), 
            where_clause, 
            self:_parse_group(), 
            self:_parse_having(), 
            self:_parse_order(), 
            self:_parse_limit(), 
            self:_parse_offset())
    end
end
function Sql.create(self, params)
    if self._create == nil then
        self._create = {}
    end
    for k, v in pairs(params) do
        self._create[k] = v
    end
    return self
end
function Sql.update(self, params)
    if self._update == nil then
        self._update = {}
    end
    for k, v in pairs(params) do
        self._update[k] = v
    end
    return self
end
function Sql.delete(self)
    self._delete = true
    return self
end
function Sql.where(self, params)
    -- in case of :where{Q{foo=1}}:where{Q{bar=2}}
    -- args must be processed seperately
    if type(params) == 'table' then
        for k, v in pairs(params) do
            if type(k) == 'number' then
                if self._where_args == nil then
                    self._where_args = {}
                end
                table_insert(self._where_args, v)
            else
                if self._where_kwargs == nil then
                    self._where_kwargs = {}
                end
                self._where_kwargs[k] = v
            end
        end
    else
        self._where_string = params
    end
    return self
end
function Sql.having(self, params)
    if type(params) == 'table' then
        if self._having == nil then
            self._having = {}
        end
        for k, v in pairs(params) do
            self._having[k] = v
        end
    else
        self._having_string = params
    end
    return self
end
function Sql.group(self, params)
    if type(params) == 'table' then
        if self._group == nil then
            self._group = {}
        end
        local res = self._group
        for i, v in ipairs(params) do
            res[#res+1] = v
        end
    else
        self._group_string = params
    end    
    return self
end
function Sql.select(self, params)
    if type(params) == 'table' then
        if self._select == nil then
            self._select = {}
        end
        local res = self._select
        for i, v in ipairs(params) do
            res[#res+1] = v
        end
    else
        self._select_string = params
    end
    return self
end
function Sql.order(self, params)
    if type(params) == 'table' then
        if self._order == nil then
            self._order = {}
        end
        local res = self._order
        for i, v in ipairs(params) do
            -- if v:sub(1, 1) == '-' then
            --     -- convert '-key' to 'key desc'
            --     v = v:sub(2)..' DESC'
            -- end
            res[#res+1] = v
        end
    else
        self._order_string = params
    end
    return self
end
function Sql.limit(self, params)
    -- only accept string
    self._limit_string = params
    return self
end
function Sql.offset(self, params)
    -- only accept string
    self._offset_string = params
    return self
end
function Sql.distinct(self)
    self._distinct = true
    return self
end
function Sql.escape_name(name)
    error('method not implemented')
end
function Sql.escape_value(value)
    error('method not implemented')
end
function Sql.escape_string(value)
    error('method not implemented')
end
function Sql._add_to_join(self, t)
    -- ** need to check logic here via more test cases
    -- ** does order matters ?
    for i, v in ipairs(self._join) do
        if t.key == v.key then
            return v.right_table_alias or v.right_table
        end
    end
    local n = #self._join + 1
    for i, v in ipairs(self._join) do
        if t.right_table == v.right_table and not t.right_table_alias then
            t.right_table_alias = 'T'..n
            break
        end
    end
    self._join[n] = t
    return t.right_table_alias or t.right_table
end

local START = 1
local FOREIGN_KEY =2
local NON_FOREIGN_KEY = 3
local END = 4
function Sql._parse_key_value(self, key, value)
    local field, template, foreign_key, last_join_name
    local model = self.model
    local prefix = model.table_name
    local operator = 'eq'
    local join_key = ''
    local state = START
    for i, e in ipairs(ngx_re_split(key, '__', 'jo')) do
        if state == START then
            local f = model.fields_dict[e]
            if f ~= nil then
                field = e 
                if model.foreign_keys[e] then
                    foreign_key = f
                    state = FOREIGN_KEY 
                else 
                    state = NON_FOREIGN_KEY 
                end
            else
                return nil, string_format('`%s` is not a valid field for %s.', e, model.table_name)
            end
        elseif state == NON_FOREIGN_KEY then
            operator = e
            state = END
        elseif state == FOREIGN_KEY then
            local fk_model = foreign_key.reference 
            local f = fk_model.fields_dict[e]
            if f ~= nil then 
                if not self._join then
                    self._join = {}
                end
                join_key = join_key..'__'..field
                last_join_name = self:_add_to_join{
                    left_table = model.table_name, 
                    left_table_alias = last_join_name,
                    right_table = fk_model.table_name, 
                    -- `not last_join_name` means it's the first join,
                    -- so use foreign key as alias for readibility
                    right_table_alias = not last_join_name and field,
                    left_field = field, 
                    key = join_key,
                }
                field = e -- this line must go after _add_to_join
                prefix = last_join_name
                if f.reference then
                    foreign_key = f
                    model = fk_model
                else 
                    state = NON_FOREIGN_KEY 
                end
            else
                operator = e
                state = END
            end
        else
            return nil, 'invalid parsing state with token:'..e
        end
    end
    local template = RELATIONS[operator] or LIKE_RELATIONS[operator] 
    if template == nil then
        return nil, 'invalid operator: '..operator 
    end
    if type(value) ~= 'table' then
        if RELATIONS[operator] then
            value = self.escape_value(value)
        else
            value = self.escape_string(value)
        end
    else
        value = '('..table_concat(map(value, self.escape_value), ", ")..')'
    end
    return string_format(template, self.escape_name(prefix)..'.'..self.escape_name(field), value)
end 
function Sql._parse_params(self, args, kwargs)
    local results = {}
    if args then -- Q object is used.
        for i, Q in ipairs(args) do
            local pair, err = Q:serialize(self)
            if err then
                return nil, err
            end
            results[#results+1] = pair
        end
    end
    if kwargs then
        for key, value in pairs(kwargs) do
            local pair, err = self:_parse_key_value(key, value)
            if err then
                return nil, err
            end
            results[#results+1] = pair
        end
    end
    return table_concat(results, " AND ")
end
function Sql._parse_select(self)
    local res = {}
    local escn = self.escape_name
    if self._select_string then
        res[#res+1] = self._select_string
    elseif self._select and self._select[1] then
        local prefix = self._quoted_table_name_prefix
        for i, v in ipairs(self._select) do
            if ngx_re_match(v, '^[_\\w]+$', 'jo') then
                res[#res+1] = prefix..escn(v)
            else -- select{'count(c)', 'c as cc'}
                res[#res+1] = v
            end
        end
    else
        -- _select_all_string_cache shuold be provided by subclass
        res[#res+1] = self._select_all_string_cache
    end
    if self._select_join then
        -- ** need test
        -- extra fields needed if Sql:join is used
        for fk, fk_model in pairs(self._select_join) do
            -- fk -> buyer, fk_model -> User, 
            for k, v in pairs(fk_model.fields_dict) do
                res[#res+1] = string_format('%s.%s AS %s__%s', escn(fk), escn(k), fk, k)
            end
        end
    end
    return table_concat(res, ', ')
end
function Sql._parse_where(self)
    if self._where_string then
        return ' WHERE '..self._where_string
    elseif self._where_args or self._where_kwargs then
        local _where_string, err = self:_parse_params(self._where_args, self._where_kwargs)
        if err then
            return nil, err
        end
        if _where_string == '' then
            return ''
        end
        return ' WHERE '.._where_string
    else
        return ''
    end
end
function Sql._parse_group(self)
    if self._group_string then
        return ' GROUP BY '..self._group_string
    elseif self._group then
        -- you should take care of column prefix stuff
        return ' GROUP BY '..table_concat(self._group, ', ')
    else
        return ''
    end
end
function Sql._parse_order(self)
    if self._order_string then
        return ' ORDER BY '..self._order_string
    elseif self._order then
        -- you should take care of column prefix stuff
        return ' ORDER BY '..table_concat(self._order, ', ')
    else
        return ''
    end
end
function Sql._parse_having(self)
    -- this is simpler than `_parse_where` because no foreign key stuff involved
    -- and no need to check a field name is valid or not
    if self._having_string then
        return ' HAVING '..self._having_string
    elseif self._having then
        local results = {}
        for key, value in pairs(self._having) do
            -- try foo__bar -> foo, bar
            local field, operator, template
            local pos = key:find('__', 1)
            if pos then
                field = key:sub(1, pos-1)
                operator = key:sub(pos+2)
            else
                field = key
                operator = 'eq'
            end
            template = RELATIONS[operator] or LIKE_RELATIONS[operator] 
            if template == nil then
                return nil, 'invalid operator:'..operator
            end
            if type(value) ~= 'table' then
                if RELATIONS[operator] then
                    value = self.escape_value(value)
                else
                    value = self.escape_string(value)
                end
            else
                value = '('..table_concat(map(value, self.escape_value), ", ")..')'
            end
            results[#results+1] = string_format(template, self.escape_name(field), value)
        end
        return ' HAVING '..table_concat(results, " AND ")
    else
        return ''
    end
end
function Sql._parse_limit(self)
    return self._limit_string  and ' LIMIT '..self._limit_string or ''
end
function Sql._parse_offset(self)
    return self._offset_string  and ' OFFSET '..self._offset_string or ''
end
function Sql._parse_from(self) 
    local escn = self.escape_name
    local from_string = self._quoted_table_name
    if self._join then
        -- k : mom, v.left: pet, v.right: user
        for i, v in ipairs(self._join) do
            from_string = string_format('%s JOIN %s %s ON (%s.%s = %s.id)', 
                from_string, 
                escn(v.right_table), -- user
                v.right_table_alias and escn(v.right_table_alias) or '', -- buyer
                escn(v.left_table_alias or v.left_table), 
                escn(v.left_field), 
                escn(v.right_table_alias or v.right_table))
        end
    end
    return from_string
end
Sql.CHAIN_METHODS = CHAIN_METHODS

return Sql