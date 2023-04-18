# About

Provides some high level functionality on top of the pre-existing etcd v3 client to fulfill the need of our various projets that need to interact with etcd.

While the etcd v3 client is very powerful and flexible, we found that it entails some amount of repetitive boilerplate code in projects that depend on it.

We aim to make our dependent projects simpler by abstracting that boilerplate into higher level reusable abstractions.

# Project Status

This project is not yet fully stable.

Tested, documented structures and methods in the sdk are less likely to change as much in the future, although we are not yet prepared to make significant backward compatibility commitments on the api yet.

The existing api surface has been battle-tested against stable etcd clusters in all of its dependent projects, though it has not been well vetted against failing etcd nodes as we haven't had to deal with those all that much yet. We aim to increase our test coverage to cover more of those scenarios in the future.