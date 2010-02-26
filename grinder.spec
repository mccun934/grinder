%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name: grinder
Version: 0.0.3
Release:        1%{?dist}
Summary: A tool synching content

Group: Development/Tools
License: GPLv2
URL: http://github.com/mccun934/grinder
Source0: http://mmccune.fedorapeople.org/grinder/grinder-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildArch: noarch
BuildRequires: python-setuptools
Requires:       python >= 2.4

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


%changelog
* Fri Feb 26 2010 Mike McCune <mmccune@redhat.com> 0.0.3-1
- Initial creation of RPM/specfile 

