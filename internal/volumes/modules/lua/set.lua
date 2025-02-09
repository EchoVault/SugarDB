
-- The keyword to trigger the command
command = "LUA.SET"

--[[
The string array of categories this command belongs to.
This array can contain both built-in categories and new custom categories.
]]
categories = {"set", "write", "fast"}

-- The description of the command
description = "([LUA.SET key member [member ...]]) \
This is an example of working with SugarDB sets in lua scripts"

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
  if (#command < 3) then
    error("wrong number of args, expected 2 or more")
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

  -- Extract the key
  local key1 = command[2]
  local key2 = command[3]
  local key3 = command[4]

  -- Create two sets for testing `move` and `subtract`
  local set1 = set.new({"elem1", "elem2", "elem3"})
  local set2 = set.new({"elem4", "elem5"})

  -- Call `add` to add elements to set1
  set1:add({"elem6", "elem7"})

  -- Call `contains` to check if an element exists in set1
  local containsElem1 = set1:contains("elem1")
  local containsElemUnknown = set1:contains("unknown")

  -- Call `cardinality` to get the size of set1
  local set1Cardinality = set1:cardinality()

  -- Call `remove` to remove elements from set1
  local removedCount = set1:remove({"elem1", "elem2"})

  -- Call `pop` to remove and retrieve elements from set1
  local poppedElements = set1:pop(2)

  -- Call `random` to get random elements from set1
  local randomElements = set1:random(1)

  -- Call `all` to retrieve all elements from set1
  local allElements = set1:all()

  -- Test `move` method: move an element from set1 to set2
  local moveSuccess = set1:move(set2, "elem3")

  -- Verify that the element was moved
  local set2ContainsMoved = set2:contains("elem3")
  local set1NoLongerContainsMoved = not set1:contains("elem3")

  -- Test `subtract` method: subtract set2 from set1
  local resultSet = set1:subtract({set2})

  -- Store the modified sets in SugarDB using setValues
  setValues({[key1] = set1, [key2] = set2, [key3] = resultSet})

  -- Retrieve the sets back from SugarDB to verify storage
  local storedValues = getValues({key1, key2, key3})
  local storedSet1 = storedValues[key1]
  local storedSet2 = storedValues[key2]
  local storedResultSet = storedValues[key3]

  -- Perform additional checks to ensure consistency
  if not storedSet1 or storedSet1:cardinality() ~= set1:cardinality() then
    error("Stored set1 does not match the modified set1")
  end
  if not storedSet2 or storedSet2:cardinality() ~= set2:cardinality() then
    error("Stored set2 does not match the modified set2")
  end
  if not storedResultSet or storedResultSet:cardinality() ~= resultSet:cardinality() then
    error("Stored result set does not match the computed result set")
  end

  -- If all operations succeed, return "+OK\r\n"
  return "+OK\r\n"
end