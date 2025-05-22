#!/bin/bash

cp debian/changelog debian/changelog.save

VERSION=6.3.1-rev1

# update the changelog file with the package version
dch -v $VERSION -m auto-build

dpkg-buildpackage -b -us -uc

mv debian/changelog.save debian/changelog
