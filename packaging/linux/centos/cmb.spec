Summary: Cloud Message Bus - a clone of SQS/SNS
Name: cmb
Version: 2.2.37
BuildArch: noarch
Release: 1
Group: Applications
License: Private
AutoReqProv: no
Source: http://cmbdownloads.s3-website-us-west-1.amazonaws.com/%{version}/cmb-distribution-%{version}.tar.gz
URL: http://github.com/Comcast/cmb


%description
Cloud Message Bus implements a (largely) API compatible implementation of Amazon's SQS and SNS services

%prep

%setup -n cmb

%build

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/opt/cmb/logs
mkdir -p %{buildroot}/etc/init.d
cp %{_builddir}/cmb/packaging/linux/centos/cmb.init %{buildroot}/etc/init.d/cmb
cp -r %{_builddir}/cmb  %{buildroot}/opt
cp %{_builddir}/cmb/packaging/common/log4j.properties %{buildroot}/opt/cmb/config/log4j.properties

%clean

%files
%defattr(-,cmb,cmb)
/opt/cmb
%attr(755, root, root) /etc/init.d/cmb
%config(noreplace) /opt/cmb/config/cmb.properties
%config(noreplace) /opt/cmb/config/log4j.properties

%pre
groupadd cmb || true
useradd -g cmb cmb || true

%post
/sbin/chkconfig --add cmb

%preun
/sbin/service cmb stop 
/sbin/chkconfig --del cmb

