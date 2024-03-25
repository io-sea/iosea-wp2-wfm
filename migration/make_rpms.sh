#!/bin/bash

rpmbuild --define "_sourcedir $PWD" -ba iosea-migration.spec 

