REM Generated by Genezzo::Havok::SysHook version 7.12 on 2007-07-04T00:45:39
REM
REM Copyright (c) 2005, 2006, 2007 Jeffrey I Cohen.  All rights reserved.
REM
REM 
select HavokUse('Genezzo::Havok::SysHook') from dual;

REM HAVOK_EXAMPLE
i sys_hook 1 Genezzo::Dict dicthook1 Howdy_Hook require Genezzo::Havok::Examples Howdy SYSTEM 2007-07-04T00:45:39 0
i sys_hook 2 Genezzo::Dict dicthook1 Ciao_Hook  require Genezzo::Havok::Examples Ciao SYSTEM 2007-07-04T00:45:39 0



commit
shutdown
startup
