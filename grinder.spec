%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name: grinder
Version: 0.0.18
Release: 1%{?dist}
Summary: A tool synching content

Group: Development/Tools
License: GPLv2
URL: http://github.com/mccun934/grinder
Source0: http://mmccune.fedorapeople.org/grinder/grinder-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildArch: noarch
BuildRequires: python-setuptools
Requires:      createrepo, python >= 2.4
Requires:      PyYAML
%description
A tool for synching content from the Red Hat Network.

%prep
%setup -q -n grinder-%{version}


%build
%{__python} setup.py build


%install
rm -rf $RPM_BUILD_ROOT
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT
rm -f $RPM_BUILD_ROOT%{python_sitelib}/*egg-info/requires.txt

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc README COPYING
%{_bindir}/grinder
%dir %{python_sitelib}/grinder
%{python_sitelib}/grinder/*
%{python_sitelib}/grinder-*.egg-info
%config(noreplace) %{_sysconfdir}/grinder/grinder.yml


%changelog
* Wed Mar 10 2010 Mike McCune <mmccune@redhat.com> 0.0.18-1
- fixing spacing (mmccune@redhat.com)
- 571452 - ParallelFetch create channel directory should be silent if the
  directory already exists (jwmatthews@gmail.com)

* Thu Mar 04 2010 Mike McCune <mmccune@redhat.com> 0.0.17-1
- add log statement to show if/where removeold package is working from
  (jmatthews@virtguest-rhq-server.localdomain)
- add option to remove old RPMs from disk (jmatthews@virtguest-rhq-
  server.localdomain)

* Wed Mar 03 2010 Mike McCune <mmccune@redhat.com> 0.0.16-1
- update dir name for /etc/grinder (jmatthews@virtguest-rhq-server.localdomain)
- add PyYAML to grinder.spec (jmatthews@virtguest-rhq-server.localdomain)
- add yaml configuration file to setuptools (jmatthews@virtguest-rhq-
  server.localdomain)
- adding yaml configuration file/parsing to grinder (jmatthews@virtguest-rhq-
  server.localdomain)
- fixing paths and moving a bit forward (mmccune@redhat.com)

* Tue Mar 02 2010 Mike McCune <mmccune@redhat.com> 0.0.14-1
- 569963 - Adding dependency on createrepo (skarmark@redhat.com)
- adding test hook (mmccune)
- Adding error handling for a system trying to run grinder without activating
  (skarmark@redhat.com)

* Fri Feb 26 2010 Mike McCune <mmccune@redhat.com> 0.0.11-1
- Initial creation of RPM/specfile 

