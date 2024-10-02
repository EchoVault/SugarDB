
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
    return "wrong number of args, expected 4"
  end
  local keys = {}
  keys["readKeys"] = {command[2]}
  keys["writeKeys"] = {command[4]}
  return keys
end