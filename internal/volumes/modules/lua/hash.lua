
-- The keyword to trigger the command
command = "LUA.HASH"

--[[
The string array of categories this command belongs to.
This array can contain both built-in categories and new custom categories.
]]
categories = {"hash", "write", "fast"}

-- The description of the command
description = "(LUA.HASH key) \
This is an example of working with SugarDB hashes/maps in lua scripts."

-- Whether the command should be synced across the RAFT cluster
sync = true

--[[
keyExtractionFunc is a function that extracts the keys from the command and returns them to SugarDB.keyExtractionFunc
The returned data from this function is used in the Access Control Layer to determine if the current connection is
authorized to execute this command. The function must return a table that specifies which keys in this command
are read keys and which ones are write keys.
Example return: {["readKeys"] = {"key1", "key2"}, ["writeKeys"] = {"key3", "key4", "key5"}}

1. "command" is a string array representing the command that triggered this key extraction function.

2. "args" is a string array of the modifier args that were passed when loading the module into SugarDB.
   These args are passed to the key extraction function everytime it's invoked.
]]
function keyExtractionFunc (command, args)
  for k,v in pairs(args) do
    print(k, v)
  end
  if (#command < 2) then
    error("wrong number of args, expected 1")
  end
  return { ["readKeys"] = {command[2]}, ["writeKeys"] = {} }
end

--[[
handlerFunc is the command's handler function. The function is passed some arguments that allow it to interact with
SugarDB. The function must return a valid RESP response or throw an error.
The handler function accepts the following args:

1. "context" is a table that contains some information about the environment this command has been executed in.
    Example: {["protocol"] = 2, ["database"] = 0}
    This object contains the following properties:
    i) protocol - the protocol version of the client that executed the command (either 2 or 3).
    ii) database - the active database index of the client that executed the command.

2. "command" is the string array representing the command that triggered this handler function.

3. "keyExists" is a function that can be called to check if a list of keys exists in the SugarDB store database.
    This function accepts a string array of keys to check and returns a table with each key having a corresponding
    boolean value indicating whether it exists.
    Examples:
    i) Example invocation: keyExists({"key1", "key2", "key3"})
    ii) Example return: {["key1"] = true, ["key2"] = false, ["key3"] = true}

4. "getValues" is a function that can be called to retrieve values from the SugarDB store database.
    The function accepts a string array of keys whose values we would like to fetch, and returns a table with each key
    containing the corresponding value from the store.
    The possible data types for the values are: number, string, nil, hash, set, zset
    Examples:
    i) Example invocation: getValues({"key1", "key2", "key3"})
    ii) Example return: {["key1"] = 3.142, ["key2"] = nil, ["key3"] = "Pi"}

5. "setValues" is a function that can be called to set values in the active database in the SugarDB store.
    This function accepts a table with keys and the corresponding values to set for each key in the active database
    in the store.
    The accepted data types for the values are: number, string, nil, hash, set, zset.
    The setValues function does not return anything.
    Examples:
    i) Example invocation: setValues({["key1"] = 3.142, ["key2"] = nil, ["key3"] = "Pi"})

6. "args" is a string array of the modifier args passed to the module at load time. These args are passed to the
   handler everytime it's invoked.
]]
function handlerFunc(context, command, keysExist, getValues, setValues, args)
  -- Initialize a new hash
  local h = hash.new()

  -- Set values in the hash
  h:set({
    {["field1"] = "value1"},
    {["field2"] = "value2"},
    {["field3"] = "value3"},
    {["field4"] = "value4"},
  })

  -- Set hash in the store
  setValues({[command[2]] = h})

  -- Check that the fields were correctly set in the database
  local hashValue = getValues({command[2]})[command[2]]
  assert(hashValue:get({"field1"})["field1"] == "value1", "field1 not set correctly")
  assert(hashValue:get({"field2"})["field2"] == "value2", "field2 not set correctly")
  assert(hashValue:get({"field3"})["field3"] == "value3", "field3 not set correctly")
  assert(hashValue:get({"field4"})["field4"] == "value4", "field4 not set correctly")

  -- Test get method
  local retrieved = h:get({"field1", "field2"})
  assert(retrieved["field1"] == "value1", "get method failed for field1")
  assert(retrieved["field2"] == "value2", "get method failed for field2")

  -- Test exists method
  local exists = h:exists({"field1", "fieldX"})
  assert(exists["field1"] == true, "exists method failed for field1")
  assert(exists["fieldX"] == false, "exists method failed for fieldX")

  -- Test setnx method
  local setnxCount = h:setnx({
    {["field1"] = "new_value1"}, -- Should not overwrite
    {["field5"] = "value5"},     -- Should set
  })
  assert(setnxCount == 1, "setnx did not set the correct number of fields")
  assert(h:get({"field1"})["field1"] == "value1", "setnx overwrote field1")
  assert(h:get({"field5"})["field5"] == "value5", "setnx failed to set field5")

  -- Test del method
  local delCount = h:del({"field2", "field3"})
  assert(delCount == 2, "del did not delete the correct number of fields")
  assert(h:exists({"field2"})["field2"] == false, "del failed to delete field2")
  assert(h:exists({"field3"})["field3"] == false, "del failed to delete field3")

  -- Test len method
  assert(h:len() == 3, "len method returned incorrect value")

  -- Retrieve and verify all remaining fields
  local remainingFields = h:all()
  assert(remainingFields["field1"] == "value1", "field1 missing after deletion")
  assert(remainingFields["field4"] == "value4", "field4 missing after deletion")
  assert(remainingFields["field5"] == "value5", "field5 missing after deletion")

  -- Return RESP response
  return "+OK\r\n"
end