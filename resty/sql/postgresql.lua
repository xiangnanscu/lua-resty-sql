local BaseSql = require "resty.sql.base"
local string_format = string.format
local table_concat = table.concat
local table_insert = table.insert
local ipairs = ipairs
local pairs = pairs
local tostring = tostring
local error = error

local version = '1.1'

local Sql = BaseSql:class{}
function Sql.class(cls, subcls)
    subcls = BaseSql.class(cls, subcls)
    subcls:bind_model(subcls.model)
    return subcls
end
function Sql.escape_name(name)
    return '"'..name..'"'
end
function Sql.escape_value(value)
    local _exp_0 = type(value)
    if "string" == _exp_0 then
        return "'" .. (value:gsub("'", "''")) .. "'"
    elseif "number" == _exp_0 then
        return tostring(value)
    elseif "boolean" == _exp_0 then
        return value and "TRUE" or "FALSE"
    end
    return error("don't know how to escape value: " .. tostring(value))
end
function Sql.escape_string(value)
    return (value:gsub("'", "''"))
end
function Sql.to_update_sql(self)
    -- **need test
    local escn = self.escape_name
    local escv = self.escape_value
    local where_clause, err = self:_parse_where()
    if err then
        return nil, err
    end
    if self._join then
        where_clause = string_format(' WHERE id IN (SELECT %s.id FROM %s%s)', 
            self._quoted_table_name, 
            self:_parse_from(), 
            where_clause)
    end
    local res = {}
    for k, v in pairs(self._update) do
        res[#res+1] = escn(k)..' = '..escv(v)
    end
    return string_format('UPDATE %s SET %s%s', 
        self._quoted_table_name, 
        table_concat(res, ', '), 
        where_clause)
end

return Sql
