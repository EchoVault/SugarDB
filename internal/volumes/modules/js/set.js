
// The keyword to trigger the command
var command = "JS.SET"

// The string array of categories this command belongs to.
// This array can contain both built-in categories and new custom categories.
var categories = ["set", "write", "fast"]

// The description of the command.
var description = "(JS.SET key member [member ...]]) " +
  "This is an example of working with SugarDB sets in js scripts."

// Whether the command should be synced across the RAFT cluster.
var sync = true

/**
 *  keyExtractionFunc is a function that extracts the keys from the command and returns them to SugarDB.keyExtractionFunc
 *  The returned data from this function is used in the Access Control Layer to determine if the current connection is
 *  authorized to execute this command. The function must return a table that specifies which keys in this command
 *  are read keys and which ones are write keys.
 *  Example return: {readKeys: ["key1", "key2"], writeKeys: ["key3", "key4", "key5"]}
 *
 *  1. "command" is a string array representing the command that triggered this key extraction function.
 *
 *  2. "args" is a string array of the modifier args that were passed when loading the module into SugarDB.
 *  These args are passed to the key extraction function everytime it's invoked.
 */
function keyExtractionFunc(command, args) {
  // Check the length of the command array
  if (command.length < 3) {
    throw new Error("wrong number of args, expected 2 or more");
  }
  // Return the result object
  return {
    readKeys: [],
    writeKeys: [command[1], command[2], command[3]]
  };
}

/**
 * handlerFunc is the command's handler function. The function is passed some arguments that allow it to interact with
 * SugarDB. The function must return a valid RESP response or throw an error.
 * The handler function accepts the following args:
 *
 * 1. "context" is a table that contains some information about the environment this command has been executed in.
 *     Example: {protocol: 2, database: 0}
 *     This object contains the following properties:
 *     i) protocol - the protocol version of the client that executed the command (either 2 or 3).
 *     ii) database - the active database index of the client that executed the command.
 *
 * 2. "command" is the string array representing the command that triggered this handler function.
 *
 * 3. "keyExists" is a function that can be called to check if a list of keys exists in the SugarDB store database.
 *     This function accepts a string array of keys to check and returns a table with each key having a corresponding
 *     boolean value indicating whether it exists.
 *     Examples:
 *     i) Example invocation: keyExists(["key1", "key2", "key3"])
 *     ii) Example return: {key1: true, key2: false, key3: true}
 *
 * 4. "getValues" is a function that can be called to retrieve values from the SugarDB store database.
 *     The function accepts a string array of keys whose values we would like to fetch, and returns a table with each key
 *     containing the corresponding value from the store.
 *     The possible data types for the values are: number, string, nil, hash, set, zset
 *     Examples:
 *     i) Example invocation: getValues(["key1", "key2", "key3"])
 *     ii) Example return: {key1: 3.142, key2: nil, key3: "Pi"}
 *
 * 5. "setValues" is a function that can be called to set values in the active database in the SugarDB store.
 *     This function accepts a table with keys and the corresponding values to set for each key in the active database
 *     in the store.
 *     The accepted data types for the values are: number, string, nil, hash, set, zset.
 *     The setValues function does not return anything.
 *     Examples:
 *     i) Example invocation: setValues({key1: 3.142, key2: nil, key3: "Pi"})
 *
 * 6. "args" is a string array of the modifier args passed to the module at load time. These args are passed to the
 *    handler everytime it's invoked.
 */
function handlerFunc(ctx, command, keysExist, getValues, setValues, args) {
  // Ensure there are enough arguments
  if (command.length < 3) {
    throw "wrong number of arguments, expected at least 3";
  }

  // Extract the keys
  var key1 = command[1];
  var key2 = command[2];
  var key3 = command[3];

  // Create two sets for testing `move` and `subtract`
  var set1 = new Set(["elem1", "elem2", "elem3"]);
  var set2 = new Set(["elem4", "elem5"]);

  // Add elements to set1
  set1.add(["elem6", "elem7"]);

  // Check if an element exists in set1
  var containsElem1 = set1.contains("elem1");
  console.assert(containsElem1, "set1 does not contain expected element elem1")
  var containsElemUnknown = set1.contains("unknown");
  console.assert(!containsElemUnknown, "set1 contains unknown element")

  // Get the size of set1
  var set1Cardinality = set1.cardinality();
  console.assert(set1Cardinality, "set1 cardinality expected 3, got " + set1Cardinality)

  // Remove elements from set1
  set1.remove(["elem1", "elem2"]);
  var removedCount = 2; // Manually track removed count

  // Pop elements from set1
  set1.add(["elem1", "elem2"]);
  var poppedElements = set1.pop(2);
  console.assert(
    poppedElements.length === 2,
    "popped elements length must be 2, got " + poppedElements.length
  )

  // Get random elements from set1
  var randomElements = set1.random(2);
  console.assert(
    randomElements.length === 2,
    "random elements length must be 2, got " + randomElements.length
  )


  // Retrieve all elements from set1
  var allElements = set1.all();
  console.assert(
    allElements.length === set1.cardinality(),
    "all elements length must be " + set1.cardinality() + ", got " + allElements.length
  )

  // Move an element from set1 to set2
  set1.add(["elem3"])
  var moveSuccess = false;
  if (set1.contains("elem3")) {
    moveSuccess = set1.move(set2, "elem3");
  }
  console.assert(moveSuccess, "element not moved from set1 to set2")

  // Verify that the element was moved
  var set2ContainsMoved = set2.contains("elem3");
  console.assert(set2ContainsMoved, "set2 does not contain expected element after move")
  var set1NoLongerContainsMoved = !set1.contains("elem3");
  console.assert(set1NoLongerContainsMoved, "set1 still contains unexpected element after move")

  // Subtract set2 from set1
  var resultSet = set1.subtract([set2]);

  // Store the modified sets
  var setVals = {}
  setVals[key1] = set1
  setVals[key2] = set2
  setVals[key3] = resultSet
  setValues(setVals);

  // Retrieve the sets back to verify storage
  var storedValues = getValues([key1, key2, key3]);
  var storedSet1 = storedValues[key1];
  var storedSet2 = storedValues[key2];
  var storedResultSet = storedValues[key3];

  // Perform additional checks to ensure consistency
  if (!storedSet1 || storedSet1.size !== set1.size) {
    throw "Stored set1 does not match the modified set1";
  }
  if (!storedSet2 || storedSet2.size !== set2.size) {
    throw "Stored set2 does not match the modified set2";
  }
  if (!storedResultSet || storedResultSet.size !== resultSet.size) {
    throw "Stored result set does not match the computed result set";
  }

  // If all operations succeed, return "OK"
  return "+OK\r\n";
}