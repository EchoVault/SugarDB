
-- The keyword to trigger the command
command = "LUA.ZSET"

--[[
The string array of categories this command belongs to.
This array can contain both built-in categories and new custom categories.
]]
categories = {"sortedset", "write", "fast"}

-- The description of the command
description = "(LUA.ZSET key member score [member score ...]) \
This is an example of working with sorted sets in lua scripts"

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
  if (#command ~= 4) then
    error("wrong number of args, expected 2")
  end
  return { ["readKeys"] = {}, ["writeKeys"] = {command[2], command[3], command[4]} }
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
function handlerFunc(ctx, command, keyExists, getValues, setValues, args)
  -- Ensure there are enough arguments
  if #command < 4 then
    error("wrong number of arguments, expected at least 3")
  end

  local key1 = command[2]
  local key2 = command[3]
  local key3 = command[4]

  -- Create `zmember` instances
  local member1 = zmember.new({value = "member1", score = 10})
  local member2 = zmember.new({value = "member2", score = 20})
  local member3 = zmember.new({value = "member3", score = 30})

  -- Create a `zset` and add initial members
  local zset1 = zset.new({member1, member2})

  -- Test `add` method with a new member
  zset1:add({member3})

  -- Test `update` method by modifying an existing member
  zset1:update({zmember.new({value = "member1", score = 15})})

  -- Test `remove` method
  zset1:remove("member2")

  -- Test `cardinality` method
  local zset1Cardinality = zset1:cardinality()

  -- Test `contains` method
  local containsMember3 = zset1:contains("member3")
  local containsNonExistent = zset1:contains("nonexistent")

  -- Test `random` method
  local randomMembers = zset1:random(2)

  -- Test `all` method
  local allMembers = zset1:all()

  -- Create another `zset` to test `subtract`
  local zset2 = zset.new({zmember.new({value = "member3", score = 30})})
  local zsetSubtracted = zset1:subtract({zset2})

  -- Store the `zset` objects in SugarDB
  setValues({
    [key1] = zset1,
    [key2] = zset2,
    [key3] = zsetSubtracted
  })

  -- Retrieve the stored `zset` objects to verify storage
  local storedValues = getValues({key1, key2, key3})
  local storedZset1 = storedValues[key1]
  local storedZset2 = storedValues[key2]
  local storedSubtractedZset = storedValues[key3]

  -- Perform consistency checks
  if not storedZset1 or storedZset1:cardinality() ~= zset1:cardinality() then
    error("Stored zset1 does not match the modified zset1")
  end
  if not storedZset2 or storedZset2:cardinality() ~= zset2:cardinality() then
    error("Stored zset2 does not match the modified zset2")
  end
  if not storedSubtractedZset or storedSubtractedZset:cardinality() ~= zsetSubtracted:cardinality() then
    error("Stored subtracted zset does not match the computed result")
  end

  -- Test `zmember` methods
  local memberValue = member1:value()
  member1:value("updated_member1")
  local updatedValue = member1:value()

  local memberScore = member1:score()
  member1:score(50)
  local updatedScore = member1:score()

  -- Return an "OK" response
  return "+OK\r\n"
end