
// The keyword to trigger the command
var command = "JS.LIST"

// The string array of categories this command belongs to.
// This array can contain both built-in categories and new custom categories.
var categories = ["list", "write", "fast"]

// The description of the command.
var description = "(JS.LIST key) This is an example of working with SugarDB lists in js scripts."

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
  if (command.length !== 2) {
    throw "wrong number of args, expected 4."
  }
  return {
    "readKeys": [],
    "writeKeys": [command[1]]
  }
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
  // Helper function to compare lists
  function compareLists(expected, actual) {
    if (expected.length !== actual.length) {
      return {
        isValid: false,
        errorMessage: "Length mismatch: expected " + expected.length + ", got " + actual.length
      };
    }
    for (var i = 0; i < expected.length; i++) {
      if (expected[i] !== actual[i]) {
        return {
          isValid: false,
          errorMessage: "Mismatch at index " + (i + 1) + ": expected " + expected[i] + ", got " + actual[i]
        };
      }
    }
    return { isValid: true };
  }

  var key = command[1]; // Adjusted for JavaScript's 0-based indexing

  // First list to set
  var initialList = ["apple", "banana", "cherry"];
  var setVals = {}
  setVals[key] = initialList
  setValues(setVals);

  // Retrieve and verify the first list
  var retrievedValues = getValues([key]);
  var retrievedList = retrievedValues[key];
  var result = compareLists(initialList, retrievedList);
  if (!result.isValid) {
    throw new Error(result.errorMessage);
  }

  // Update the list with new values
  var updatedList = ["orange", "grape", "watermelon"];
  setVals = {}
  setVals[key] = updatedList
  setValues(setVals);

  // Retrieve and verify the updated list
  retrievedValues = getValues([key]);
  retrievedList = retrievedValues[key];
  result = compareLists(updatedList, retrievedList);
  if (!result.isValid) {
    throw result.errorMessage;
  }

  // If all assertions pass
  return "+OK\r\n";
}