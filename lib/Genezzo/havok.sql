REM Copyright (c) 2004 Jeffrey I Cohen.  All rights reserved.
REM
REM 
ct havok hid=n modname=c owner=c creationdate=c
ct UserExtend xid=n xtype=c xname=c args=c owner=c creationdate=c
i havok 1 Genezzo::Havok::UserExtend SYSTEM 2004-09-21T12:12

REM 
REM select * from tab1 where Genezzo::Havok::RedGreen::isRedGreen(col1)
i UserExtend 1 require Genezzo::Havok::RedGreen qw(isRedGreen) SYSTEM 2004-09-21T12:12
i UserExtend 1 require Text::Soundex qw(soundex) SYSTEM 2004-09-21T12:12

REM select * from tab1 where isBlueYellow(col1)
i UserExtend 2 function isBlueYellow '{return undef unless scalar(@_);   return ($_[0] =~ m/^(blue|yellow)$/i); }' SYSTEM 2004-09-21T12:12


commit
shutdown
startup

