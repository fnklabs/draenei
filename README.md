# draenei
Another one ORM for Cassandra but with distributed cache and analytics utils from package

Project exists in R&D mode

## Add it to your project 

### Gradle 

```
compile "com.fnklabs:fnklabs-draenei:$vers.draenei"
```

# Annotations
There are several annotations that make help your to mark your entity 

## Column

Column name, value is optional by default will use field name as value

```java
@Table
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
@Table
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

Entity can contains several partition keys in this case you must set order value for each primary key
 
```java
@Table
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
@Table
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
@Table
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
``` 
All other fields in Table annotation is optional and not used in current implementation

## Entity (Complete class)
All field values read and write though getters and setters so you must define them

```java
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


## Create your first dataProvider

```java
public class UserDataProvider extends DataProvider<User> {
    public User(CassandraClientFactory cassandraClientFactory) {
        super(User.class, cassandraClientFactory);
    }
}
```

## Saving data

```java
public class Example {
    
    
    public static void main(String[] args) {
        UserDataProvider dataProvider = new UserDataProvider(cassandraClientFactory);
        
        User user = new User();
        user.setEmail("test@example.com");
        
        ListenableFuture<Boolean> saveFuture = dataProvider.saveAsync(user);
    }
}
```

## Find data

When calling find method you must provide all partition keys

```java
public class Example {
    
    public static void main(String[] args) {
        UserDataProvider dataProvider = new UserDataProvider(cassandraClientFactory);
        
        List<User> users = dataProvider.find("test@example.com"); // sync
        ListenableFuture<List<User>> findUsersFuture = dataProvider.findAsync("test@example.com"); // async
        
        // retrieving only one record
        
        User user = dataProvider.findOne("test@example.com"); // sync
        ListenableFuture<User> findUserFuture = dataProvider.findOneAsync("test@example.com"); // async
    }
}
```

## Removing data

To remove data you must provide entity object

```java
public class Example {
    
    public static void main(String[] args) {
        UserDataProvider dataProvider = new UserDataProvider(cassandraClientFactory);
        
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