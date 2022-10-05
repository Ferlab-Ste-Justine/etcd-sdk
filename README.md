# About

We developed a couple of projects that are interacting with etcd.

There is some repetitive boilerplate between projects to abstract away lower level details of the etcd client.

This module is meant to centralise all that boilerplate in one location and keep the dependent projects simpler.

# Project Status

This project is new and not yet stable.

We still have a couple more things to import from other projects and all the api surface has not been fully validated yet.

Furthermore, we are not yet completely happy with the api contracts and will probably break many them in the future in a backward-incompatible manner.

Use at your own risk.