
// The keyword to trigger the command
var command = "JS.ZSET"

// The string array of categories this command belongs to.
// This array can contain both built-in categories and new custom categories.
var categories = ["sortedset", "write", "fast"]

// The description of the command.
var description = "(JS.ZSET key member score [member score ...]) " +
  "This is an example of working with SugarDB sorted sets in js scripts."

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
  if (command.length < 4) {
    throw "wrong number of args, expected 3 or more";
  }
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
  if (command.length < 4) {
    throw new Error("wrong number of arguments, expected at least 3");
  }

  var key1 = command[1];
  var key2 = command[2];
  var key3 = command[3];

  // Create `ZMember` instances
  var member1 = new ZMember({ value: "member1", score: 10 });
  var member2 = new ZMember({ value: "member2", score: 20 });
  var member3 = new ZMember({ value: "member3", score: 30 });

  // Create a `ZSet` and add initial members
  var zset1 = new ZSet(member1, member2);

  // Test `add` method with a new member
  zset1.add([member3]);

  // Test `update` method by modifying an existing member
  zset1.update([new ZMember({ value: "member1", score: 15 })]);

  // Test `remove` method
  zset1.remove("member2");

  // Test `cardinality` method
  var zset1Cardinality = zset1.cardinality();

  // Test `contains` method
  var containsMember3 = zset1.contains("member3");
  var containsNonExistent = zset1.contains("nonexistent");

  // Test `random` method
  var randomMembers = zset1.random(2);

  // Test `all` method
  var allMembers = zset1.all();

  // Create another `ZSet` to test `subtract` manually
  var zset2 = new ZSet(new ZMember({ value: "member3", score: 30 }));

  // Store the `ZSet` objects in SugarDB
  var setVals = {}
  setVals[key1] = zset1
  setVals[key2] = zset2
  setValues(setVals);

  // Retrieve the stored `ZSet` objects to verify storage
  var storedValues = getValues([key1, key2, key3]);
  var storedZset1 = storedValues[key1];
  var storedZset2 = storedValues[key2];

  // Perform consistency checks
  if (!storedZset1 || storedZset1.cardinality() !== zset1.cardinality()) {
    throw "Stored zset1 does not match the modified zset1";
  }
  if (!storedZset2 || storedZset2.cardinality() !== zset2.cardinality()) {
    throw "Stored zset2 does not match the modified zset2";
  }

  // Test `ZMember` methods
  var memberValue = member1.value();
  member1.value("updated_member1");
  var updatedValue = member1.value();

  var memberScore = member1.score();
  member1.score(50);
  var updatedScore = member1.score();

  // Return an "OK" response
  return "+OK\r\n";
}