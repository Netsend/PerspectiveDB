# Contributing

## First clone and accept the CAA

Fork, then clone the repo:

    git clone git@github.com:your-username/mastersync.git

Install dependencies:

    cd mastersync && npm install

Add our repo as a remote:

    git remote add upstream git@github.com:Netsend/mastersync.git

Please first read the [Contributor Assignment Agreement](https://raw.githubusercontent.com/Netsend/mastersync/master/misc/caa/caa.txt).

If you agree, create a new topic branch and sign the CAA:

    git checkout -b caa

Copy `misc/caa/caa.txt` to `misc/caa/signed/`. Make sure your e-mail address appears in the filename with the `@` and every `.` replaced by `_`. E.g. if your e-mail is `john.doe@example.com` then name the file `misc/caa/signed/caa-john_doe_example_com.txt`. Sign it by filling in your e-mail address and full name as they appear in your commit messages.

Commit the new signed CAA with in the first line of the commit message only "I Agree to the Contributor Assignment Agreement":

    git add misc/caa/signed/caa-john_doe_example_com.txt
    git commit -m 'I Agree to the Contributor Assignment Agreement'

Push your topic branch to your fork and [submit a pull request][pr]:

    git push origin caa

Wait for us to accept your pull request and signed CAA.

You can delete your local caa branch:

    git checkout master
    git branch -D caa

## Once your signed CAA is accepted

Update your repo:

    git checkout master
    git pull upstream/master

Make sure the tests pass:

    mocha

Create a topic branch, possibly with a GitHub issue number in it:

    git checkout -b GHxxx

Make your change. Add a test for your patch that fails before patching and passes after applying your patch.

    mocha

Make sure your patch is based on the latest commit in our master branch:

    git fetch mastersync
    git rebase mastersync/master

If your patch is rebased, please make sure the tests still pass:

    mocha

Push your topic branch to your fork and [submit a pull request][pr].

[pr]: https://github.com/Netsend/mastersync/compare/

At this point you're waiting on us.

Some things that will increase the chance that your pull request is accepted:

* Follow our coding style.
* Write a [good commit message][commit].

[commit]: http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
