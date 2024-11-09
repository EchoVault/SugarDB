
command = "LUA.HASH"

module = "hash"

categories = {"hash", "write", "fast"}

description = "(LUA.HASH key field value [field value ...]) \
This is an example of working with SugarDB hashes/maps in lua scripts."

sync = true

function keyExtractionFunc (command, args)
  for k,v in pairs(args) do
    print(k, v)
  end
  if (#command < 4) then
    error("wrong number of args, expected 3")
  end
  return { ["readKeys"] = {}, ["writeKeys"] = {} }
end

function handlerFunc(ctx, command, keysExist, getValues, setValues, args)
  h = hash.new()
  h:set({
    {["field1"] = "value1"},
    {["field2"] = "value2"},
    {["field3"] = "value3"},
    {["field4"] = "value4"},
  })
  print("Before delete: ", h:len())
  h:del({"field2", "field3"})
  print("After delete: ", h:len())
  return "+OK\r\n"
end