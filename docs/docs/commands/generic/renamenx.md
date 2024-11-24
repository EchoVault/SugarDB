import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RENAMENX

### Syntax
```
RENAMENX key newkey
```

### Module
<span className="acl-category">generic</span>

### Categories 
<span className="acl-category">fast</span>
<span className="acl-category">keyspace</span>
<span className="acl-category">write</span>

### Description
Renames the specified key with the new name only if the new name does not already exist.

### Examples

<Tabs
  defaultValue="go"
  values={[
    { label: 'Go (Embedded)', value: 'go', },
    { label: 'CLI', value: 'cli', },
  ]}
>
  <TabItem value="go">
  Rename the key `mykey` to `newkey`:
  ```go
  db, err := sugardb.NewSugarDB()
  if err != nil {
    log.Fatal(err)
  }
  err = db.RenameNX("mykey", "newkey")
  ```
  </TabItem>
  <TabItem value="cli">
  Rename the key `mykey` to `newkey`:
  ```
  > RENAMENX mykey newkey
  ```
  </TabItem>
</Tabs> 
