local BaseSql = require "resty.sql.base"
local string_format = string.format
local table_concat = table.concat
local table_insert = table.insert
local ipairs = ipairs
local pairs = pairs
local tostring = tostring

local Sql = BaseSql:class{}
function Sql.class(cls, subcls)
    subcls = BaseSql.class(cls, subcls)
    subcls:bind_model(subcls.model)
    return subcls
end
function Sql.escape_name(value)
    return '`'..value..'`'
end
Sql.escape_value = ngx.quote_sql_str
-- function Sql.escape_value(value)
--     if type(value) == 'string' then
--         return "'"..(value:gsub('\\', '\\\\'):gsub("'", "\\'")).."'"
--     else
--         return tostring(value)
--     end
-- end
function Sql.escape_string(value)
    return (value:gsub('\\', '\\\\'):gsub("'", "\\'"))
end
function Sql.to_update_sql(self)
    local where_clause, err = self:_parse_where()
    if err then
        return nil, err
    end
    local from_clause = self:_parse_from()
    local escn = self.escape_name
    local escv = self.escape_value
    local res = {}
    local prefix = escn(self.model.table_name)..'.'
    for k, v in pairs(self._update) do
        res[#res+1] = prefix..escn(k)..' = '..escv(v)
        -- res[#res+1] = escn(k)..' = '..escv(v)
    end
    return string_format('UPDATE %s SET %s%s;', from_clause, table_concat(res, ', '), where_clause)
end

return Sql
