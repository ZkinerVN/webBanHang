local stock_key = KEYS[1]
local quantity = tonumber(ARGV[1])

local current_stock = tonumber(redis.call('get', stock_key) or '-1')

if current_stock == -1 then
    return -1 -- Key chưa tồn tại (chưa warm cache)
end

if current_stock >= quantity then
    redis.call('decrby', stock_key, quantity)
    return 1 -- Trừ kho thành công
else
    return 0 -- Hết hàng (Out of stock)
end