import React from "react";
import CodeBlock from "@theme/CodeBlock";

const LoadModuleDocs = ({ module }: { module: "go" | "lua" | "js" }) => {
  const module_path =
    module === "go"
      ? "path/to/module/module.so"
      : module === "lua"
      ? "path/to/module/module.lua"
      : "path/to/module/module.js";

  return (
    <div>
      <p>You can load modules in 3 ways:</p>

      <h3 id="section-3-1">1. At startup with the `--loadmodule` flag.</h3>
      <p>
        Upon startup you can provide the flag {module_path}. This is the path to
        the module's file. You can pass this flag multiple times to load
        multiple modules on startup.
      </p>

      <h3 id="section-3-2">2. At runtime with the `MODULE LOAD` command.</h3>
      <p>
        You can load modules dynamically at runtime using the `MODULE LOAD`
        command as follows:
      </p>
      <CodeBlock language={"sh"}>{`MODULE LOAD ${module_path}`}</CodeBlock>
      <p>
        This command only takes one path so if you have multiple modules to
        load, You will have to load them one at a time.
      </p>

      <h3>3. At runtime the `LoadModule` method.</h3>
      <p>
        You can load a module .so file dynamically at runtime using the{" "}
        <a
          target="_blank"
          href="https://pkg.go.dev/github.com/echovault/echovault@v0.10.1/echovault#EchoVault.LoadModule"
        >
          `LoadModule`
        </a>{" "}
        method in the embedded API.
      </p>
      <CodeBlock
        language={"go"}
      >{`err = server.LoadModule("${module_path}")`}</CodeBlock>

      <h3>Loading Module with Args</h3>
      <p>
        You might have notices the `args ...string` variadic parameter when
        creating a module. This a list of args that are passed to the module's
        key extraction and handler functions.
      </p>
      <p>
        The values passed here are established once when loading the module, and
        the same values will be passed to the respective functions everytime the
        command is executed.
      </p>
      <p>
        If you don't provide any args, an empty slice will be passed in the args
        parameter. Otehrwise, a slice containing your defined args will be used.
      </p>
      <p>To load a module with args using the embedded API: </p>
      <CodeBlock language={"go"}>
        {`err = server.LoadModule("${module_path}", "list", "of", "args")`}
      </CodeBlock>
      <p>To load a module with args using the `MODULE LOAD` command:</p>
      <CodeBlock language={"sh"}>
        {`MODULE LOAD ${module_path} arg1 arg2 arg3`}
      </CodeBlock>
      <p>
        NOTE: You cannot pass args when loading modules at startup with the
        `--loadmodule` flag.
      </p>

      <h2>List Modules</h2>
      <p>
        You can list the current modules loaded in the SugarDB instance using
        both the Client-Server and embedded APIs.
      </p>
      <p>
        To check the loaded modules using the embedded API, use the{" "}
        <a
          target="_blank"
          href="https://pkg.go.dev/github.com/echovault/echovault@v0.10.1/echovault#EchoVault.ListModules"
        >
          `ListModules`
        </a>{" "}
        method:
      </p>
      <CodeBlock language={"go"}>{`modules := server.ListModules()`}</CodeBlock>
      <p>
        This method returns a string slice containing all the loaded modules in
        the SugarDB instance.
      </p>
      <p>
        You can also list the loaded modules over the TCP API using the `MODULE
        LIST` command.
      </p>
      <p>Here's an example response of the loaded modules:</p>
      <CodeBlock language={"sh"}>{`1) "acl"
2) "admin"
3) "connection"
4) "generic"
5) "hash"
6) "list"
7) "pubsub"
8) "set"
9) "sortedset"
10) "string"
11) "${module_path}"`}</CodeBlock>
      <p>
        Notice that the modules loaded from .so files have their respective file
        names as the module name.
      </p>

      <h2>Execute Module Command</h2>
      <p>
        Here's an example of executing the `Module.Set` command with the
        embedded API:
      </p>
      <p>
        Here's an example of executing the COPYDEFAULT custom command that we
        created previously:
      </p>
      <CodeBlock language={"go"}>{`// Execute the custom COPYDEFAULT command
res, err := server.ExecuteCommand("Module.Set", "key1", "10")
if err != nil {
  fmt.Println(err)
} else {
  fmt.Println(string(res))
}`}</CodeBlock>
      <p>
        Here's how we would exectute the same command over the TCP client-server
        interface:
      </p>
      <CodeBlock language={"sh"}>{`Module.Set key1 10`}</CodeBlock>

      <h2>Unload Module</h2>
      <p>
        You can unload modules from the SugarDB instance using both the embedded
        and TCP APIs.
      </p>
      <p>Here's an example of unloading a module using the embedded API:</p>
      <CodeBlock language="go">{`// Unload custom module
server.UnloadModule("${module_path}")
// Unload built-in module
server.UnloadModule("sortedset")`}</CodeBlock>
      <p>Here's an example of unloading a module using the TCP interface:</p>
      <CodeBlock language="sh">{`MODULE UNLOAD ${module_path}`}</CodeBlock>
      <p>
        When unloading a module, the name should be equal to what's returned
        from the `ListModules` method or the `ModuleList` command.
      </p>
    </div>
  );
};

export default LoadModuleDocs;
