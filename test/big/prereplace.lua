box.prereplace_lua_test_trigger_behaviour = 'pass' 

box.prereplace_lua_test_trigger = function(space, old_tuple, new_tuple)
    if box.prereplace_lua_test_trigger_behaviour == 'error' then
        error(
            "space[" .. tostring(space.n) .. "].prereplace_trigger(" ..
                type(old_tuple) .. ", " .. type(new_tuple) .. ")"
        )
    end

    if box.prereplace_lua_test_trigger_behaviour == 'oldtuple' then
        if new_tuple ~= nil and old_tuple ~= nil then
            if old_tuple[0] == new_tuple[0] then
                error "dont replace the tuple!"
            end
        end
    end
    if box.prereplace_lua_test_trigger_behaviour == 'insert' then
        if new_tuple ~= nil and old_tuple == nil then
            error "dont insert the tuple!"
        end
    end
end
