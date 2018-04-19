At the very beginning, initialize the project.

    git init
    git add script/build
    git commit -am "Initial commit."
    git tag -a 0.0.0 -m "Initial commit."
    echo 0.0.0 > VERSION
    git add VERSION
    git ci -m "Add VERSION" VERSION

Update VERSION file and commit.

    script/build/update_version

Update version strings in build.boot, README, and elsewhere.

    script/build/update_strings

Tag release based on VERSION.

    script/build/tag_release


Note that version strings in build.boot, README, and in the tag won't
match the number of revisions: the generated version number is just
that: a way of generating a version string. The fact that it matches
the number of revisions since the v0.0 tag is convenient but
coincidental.
