
fiber = require('fiber')
socket = require('socket')
log = require('log')

local host = '127.0.0.1'
local port = 2003

fstat = function()
    local sock = socket('AF_INET', 'SOCK_DGRAM', 'udp')
    while true do
        local ts = tostring(math.floor(fiber.time()))
        info = {
            insert = box.stat.INSERT.rps,
            select = box.stat.SELECT.rps,
            update = box.stat.UPDATE.rps,
            delete = box.stat.DELETE.rps
        }

        for k, v in pairs(info) do
            sock:sendto(host, port, 'tnt.' .. k .. ' ' .. tostring(v) .. ' ' .. ts)
        end

        fiber.sleep(1)
        log.info('send stat ' .. ts)
    end
end

fiber.create(fstat)
