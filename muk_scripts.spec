%define logdir %{_localstatedir}/log
%define _unpackaged_files_terminate_build 0

Summary: UGent HPC scripts that should be deployed on the muk masters
Name: muk_scripts
Version: 0.1
Release: 1
License: GPL
Group: Applications/System
Source: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Requires: python-vsc-base, python-vsc-core, python-vsc-packages-lockfile, python-vsc-ldap, python-vsc-ldap-extension, pbs_python >= 4.3
BuildArch: noarch

%description
Scripts that run on one or more masters
 - GPFS quota checking and caching
 - Queue information caching for the users
 - PBS queue monitoring for inactive users

%prep
%setup -q

%build

%install
mkdir -p %{buildroot}/%{logdir}
chmod 750 %{buildroot}/%{logdir}
mkdir -p %{buildroot}/%{logdir}/pickles
chmod 750 %{buildroot}/%{logdir}/pickles
mkdir -p %{buildroot}/%{logdir}/quota/pickles
chmod 750 %{buildroot}/%{logdir}/quota/pickles
mkdir -p %{buildroot}/usr/bin/
chmod 755 %{buildroot}/usr/bin/

install -m 750 quota_check_user_notification.py %{buildroot}/usr/bin/
install -m 750 dshowq.py %{buildroot}/usr/bin
install -m 750 pbs_check_inactive_user_jobs.py %{buildroot}/usr/bin

%clean
rm -rf %{buildroot}

%files
%defattr(750,root,root,-)
%dir %{logdir}/quota
%dir %{logdir}/pickles
%{_bindir}/quota_check_user_notification.py
%{_bindir}/dshowq.py
%{_bindir}/pbs_check_inactive_user_jobs.py

%ghost %{_bindir}/quota_check_user_notification.pyc
%ghost %{_bindir}/quota_check_user_notification.pyo
%ghost %{_bindir}/dshowq.pyc
%ghost %{_bindir}/dshowq.pyo
%ghost %{_bindir}/pbs_check_inactive_user_jobs.pyc
%ghost %{_bindir}/pbs_check_inactive_user_jobs.pyo

%changelog
* Wed Feb 07 2013 Andy Georges <andy.georges@ugent.be>
- First version, copied from master_scripts
