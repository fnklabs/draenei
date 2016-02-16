# draenei
Another one ORM for Cassandra but with distributed cache and analytics utils from package

Project exists in R&D mode

## Add it to your project 

### Gradle 

```
compile 'com.fnklabs:fnklabs-draenei:0.1.2'
```

### Or as git submodule (preferred)

```bash
git submodule add https://github.com/fnklabs/draenei.git draenei
```

# Annotations
There are several annotations that make help your to mark your entity 

## Column

Column name, value is optional by default will use field name as value

```java
class User {
    @Column(name = "value")
    private String name;
    
    @Column
    private String surname;
}
```

## PrimaryKey

Each entity must contains at least one primary key (partition key).

```java
class User {
    @PrimaryKey
    @Column
    private String email;

    @Column(name = "value")
    private String name;
    
    @Column
    private String surname;
}
```

Entity can contains several partition keys in this case you must set order value for each partition
 
```java
class User {
    @PrimaryKey
    @Column
    private String email;
    
    @PrimaryKey(order = 1)
    @Column
    private String email;

    @Column(name = "value")
    private String name;
    
    @Column
    private String surname;
}
```

By default all primary keys belong to partition group if you want to mark some primary keys as clustering set PrimaryKey.isPartitionKey = false

```java
class User {
    @PrimaryKey
    @Column
    private String email;
    
    @PrimaryKey(isPartitionKey = false, order = 1)
    @Column
    private String phone;

    @Column(name = "value")
    private String name;
    
    @Column
    private String surname;
}
```

## Enumerated

To use enum fields you must set Enumerated.enumType value (need for serialization, deserialization). In current implementation all enum fields stored as text values

```java
class User {
    @Enumerated(enumType = UserRole.class)
    @Column
    private UserRole surname;
}

enum UserRole{
    ADMINISTRATOR,
    CUSTOMER
}
```

## Table

```java

@Table(name = "user", fetchSize = 100, readConsistencyLevel = ConsistencyLevel.QUORUM, writeConsistencyLevel = ConsistencyLevel.QUORUM)
class User {
    @PrimaryKey
    @Column
    private String email;

}

All other fields in Table annotation is optional and not used in current implementation

``` 

## Entity (Complete class)

```java

All field values read and write though getters and setters so you must define them

@Table(name = "user")
class User {
    @PrimaryKey
    @Column
    private String email;

    public String getEmail() {
        return email;
    }
    
    public void setEmail(String email) {
        this.email = email;
    }
}

``` 


# DataProvider

There are two several implementation of DataProvider:
1. com.fnklabs.draenei.orm.DataProvider (base implementation)
2. com.fnklabs.draenei.orm.CacheableDataProvider (use data grid for caching)

## DataProvider

### Create your first dataProvider

```java
public class UserDataProvider extends DataProvider<User> {
    public User(CassandraClientFactory cassandraClientFactory, ExecutorService executorService) {
        super(User.class, cassandraClientFactory, executorService);
    }
}
```

### Saving data

```java
public class Example {
    private static CassandraClientFactory = // implement CassandraClientFactory;
    private static ExecutorService = // retrieve executor service for data provider;
    
    public static void main(String[] args) {
        UserDataProvider dataProvider = new UserDataProvider(cassandraClientFactory, executorService);
        
        User user = new User();
        user.setEmail("test@example.com");
        
        ListenableFuture<Boolean> saveFuture = dataProvider.saveAsync(user);
    }
}
```

### Find data

When calling find method use must provide all partition keys

```java
public class Example {
    private static CassandraClientFactory = // implement CassandraClientFactory;
    private static ExecutorService = // retrieve executor service for data provider;
    
    public static void main(String[] args) {
        UserDataProvider dataProvider = new UserDataProvider(cassandraClientFactory, executorService);
        
        List<User> users = dataProvider.find("test@example.com"); // sync
        ListenableFuture<List<User>> findUsersFuture = dataProvider.findAsync("test@example.com"); // async
        
        // retrieving only one record
        
        User user = dataProvider.findOne("test@example.com"); // sync
        ListenableFuture<User> findUserFuture = dataProvider.findOneAsync("test@example.com"); // async
    }
}
```

### Removing data

To remove data you must provide entity object

```java
public class Example {
    private static CassandraClientFactory = // implement CassandraClientFactory;
    private static ExecutorService = // retrieve executor service for data provider;
    
    public static void main(String[] args) {
        UserDataProvider dataProvider = new UserDataProvider(cassandraClientFactory, executorService);
        
        User user = new User();
        user.setEmail("test@example.com");
        
        ListenableFuture<Boolean> removeFuture = dataProvider.removeAsync(user);
    }
}
```


## CacheableDataProvider
# Analytics
## Load data from cassandra into cache
## Compute operations