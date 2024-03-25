Summary:   Script to migrate data from the filesystem to IO-SEA datasets
Name:      iosea-migration
Version:   1.0
Release:   1
License:   GPL
Group:     None          
Source: iosea-migrate.sh
BuildArchitectures: noarch

%install
mkdir -p %{buildroot}/p/scratch/iosea/migration
cp %{SOURCE0} %{buildroot}/p/scratch/iosea/migration

%description
Script to migrate data from the filesystem to IO-SEA datasets

%files
/p/scratch/iosea/migration
