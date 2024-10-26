
command = "LUA.ZSET"

module = "sortedset"

categories = {"sortedset", "write", "fast"}

description = "(LUA.ZSET key member score [member score ...]) \
This is an example of working with sorted sets in lua scripts"

sync = true

function keyExtractionFunc (command, args)
  for k,v in pairs(args) do
    print(k, v)
  end
  if (#command ~= 4) then
    error("wrong number of args, expected 3")
  end
  return { ["readKeys"] = {}, ["writeKeys"] = {} }
end

function handlerFunc(ctx, command, keysExist, getValues, setValues, args)
  -- Test sorted set
  m1 = zmember.new({
    ['value'] = 'member1',
    ['score'] = 24.897
  })
  m1:value("member1-new")
  print(m1:value(), m1:score())
  m1:score(23)
  print(m1:value(), m1:score())

  ss = zset.new({m1})
  print("zupdate: ", ss:update({m1}, {['exists'] = true, ['changed'] = true, ['incr'] = true}))

  return "+OK\r\n"
end