Contributing
============

If you would like to contribute code to Squalor you can do so through
GitHub by forking the repository and sending a pull request.

When submitting code, please make every effort to follow existing
conventions and style in order to keep the code as readable as
possible. Please also make sure your code compiles and the tests pass
by running `./integration_test.sh`. The code must also be formatted
with `go fmt`.

Before your code can be accepted into the project you must also sign the
[Individual Contributor License Agreement (CLA)][1].


 [1]: https://spreadsheets.google.com/spreadsheet/viewform?formkey=dDViT2xzUHAwRkI3X3k5Z0lQM091OGc6MQ&ndplr=1


## Setting Up

Start by installing [Docker](https://docs.docker.com/installation/). And as of
now, the setup sequence is:

    boot2docker init
    boot2docker start
    eval "$(boot2docker shellinit)"

And verify everything works:

    docker run hello-world

Then you'll need a few libraries:

    go get -t

And you're ready to rock:

    ./integration_test.sh
