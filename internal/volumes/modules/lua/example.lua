
command = "LUA.EXAMPLE"

categories = {"generic", "write", "fast"}

description = "(LUA.EXAMPLE) Example lua command that sets various data types to keys"

sync = true

function keyExtractionFunc (command, args)
  for k,v in pairs(args) do
    print(k, v)
  end
  if (#command ~= 1) then
    error("wrong number of args, expected 0")
  end
  return { ["readKeys"] = {}, ["writeKeys"] = {} }
end

function handlerFunc(ctx, command, keysExist, getValues, setValues, args)
  return "+OK\r\n"
end