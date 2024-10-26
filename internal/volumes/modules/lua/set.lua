
command = "LUA.SET"

module = "set"

categories = {"set", "write", "fast"}

description = "([LUA.SET key member [member ...]]) \
This is an example of working with SugarDB sets in lua scripts"

sync = true

function keyExtractionFunc (command, args)
  for k,v in pairs(args) do
    print(k, v)
  end
  if (#command < 3) then
    error("wrong number of args, expected 2")
  end
  return { ["readKeys"] = {}, ["writeKeys"] = {} }
end

function handlerFunc(ctx, command, keysExist, getValues, setValues, args)
  -- Test set
  s1 = set.new({"a", "b", "c"})
  s1:add({"a", "a", "b", "c", "d"}) -- a, b, c, d
  print("set", s1)
  print("set:contains", s1:contains("a")) -- true
  print("set:cardinality", s1:cardinality()) -- 4
  print("set:getRandom", s1:random(1))
  s2 = set.new()
  s2:add({"c", "c", "d", "d", "e", "e", "f", "f", "g", "g"}) -- c, d, e, f, g
  s2:pop(1) -- c, d, e, f
  print("set:remove", s2:remove({"f", "g", "h", "i", "j"})) -- 1
  s2:add({"x"})
  print("set:move", s2:move(s1, "x")) -- true
  print("set:move", s2:move(s1, "z")) -- false
  s3 = set.new()
  s3:add({"x", "y", "z"})
  s4 = s1:subtract({s3})
  print("set:all", s4:all())

  return "+OK\r\n"
end