
command = "LUA.EXAMPLE"

module = "generic"

categories = {"generic", "write", "fast"}

description = "([LUA.EXAMPLE readKey value1 writeKey value2]) \
This is an example module that adds a commands using Lua scripts"

sync = true

--[[
If there's an error, return a string.
Otherwise, return a table with the "readKeys" and "writeKeys" set to arrays
of the keys that will be read from and written to respectively.
--]]
function keyExtractionFunc (command, args)
  for k,v in pairs(args) do
    print(k, v)
  end
  if (#command ~= 5) then
    return nil, "wrong number of args, expected 4"
  end
  local keys = {}
  keys["readKeys"] = {command[2]}
  keys["writeKeys"] = {command[4]}
  return keys, nil
end

function handlerFunc(ctx, command, keysExist, getValues, setValues, args)
  -- Test set
  --s1 = set.new({"a", "b", "c"})
  --s1:add({"a", "a", "b", "c", "d"}) -- a, b, c, d
  --print("set", s1)
  --print("set:contains", s1:contains("a")) -- true
  --print("set:cardinality", s1:cardinality()) -- 4
  --print("set:getRandom", s1:getRandom(1))
  --s2 = set.new()
  --s2:add({"c", "c", "d", "d", "e", "e", "f", "f", "g", "g"}) -- c, d, e, f, g
  --s2:pop(1) -- c, d, e, f
  --print("set:remove", s2:remove({"f", "g", "h", "i", "j"})) -- 1
  --s2:add({"x"})
  --print("set:move", s2:move(s1, "x")) -- true
  --print("set:move", s2:move(s1, "z")) -- false
  --s3 = set.new()
  --s3:add({"x", "y", "z"})
  --s4 = s1:subtract({s3})
  --print("set:getAll", s4:getAll())

  -- Test sorted set
  m1 = zmember.new({
    ['value'] = 'member1',
    ['score'] = 24.897
  })
  m1:value("member1-new")
  print(m1:value(), m1:score())
  m1:score(23)
  print(m1:value(), m1:score())

  return "+OK\r\n", nil
end