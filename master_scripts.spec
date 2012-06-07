%define logdir %{_localstatedir}/log/quota
%define _unpackaged_files_terminate_build 0

Summary: UGent HPC scripts that should be deployed on the masters
Name: master_scripts
Version: 0.2.12
Release: 1
License: GPL
Group: Applications/System
Source: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Requires: python-vsc-packages-common, python-vsc-packages-logging, python-vsc-packages-gpfs, python-vsc-packages-lockfile, python-vsc-packages-utils, python-vsc-packages-core, python-vsc-packages-exceptions, python-vsc-packages-ldap
%description
Scripts that run on one or more masters
 - GPFS quota checking and caching
 - Queue information caching for the users

%prep
%setup -q

%build

%install
mkdir -p %{buildroot}/%{logdir}
chmod 750 %{buildroot}/%{logdir}
mkdir -p %{buildroot}/%{logdir}/pickles
chmod 750 %{buildroot}/%{logdir}/pickles
mkdir -p %{buildroot}/usr/bin/
chmod 755 %{buildroot}/usr/bin/

install -m 750 quota_check_user_notification.py %{buildroot}/usr/bin/
install -m 750 dshowq.py %{buildroot}/usr/bin

%clean
rm -rf %{buildroot}

%files
%defattr(750,root,root,-)
%dir %{logdir}
%dir %{logdir}/pickles
%{_bindir}/quota_check_user_notification.py
%{_bindir}/dshowq.py

%ghost %{_bindir}/quota_check_user_notification.pyc
%ghost %{_bindir}/quota_check_user_notification.pyo
%ghost %{_bindir}/dshowq.pyc
%ghost %{_bindir}/dshowq.pyo

%changelog
* Wed May 06 2012 Andy Georges <andy.georges@ugent.be>
- Using a NagiosReporter for allwoing nagios checks
- All nagios check pickle files should be in the same location (e.g., /var/log/nagios/)
* Thu Apr 05 2012 Andy Georges <andy.georges@ugent.be>
- Moved to master_scripts.
* Tue Mar 20 2012 Andy Georges <andy.georges@ugent.be>
- First version 
