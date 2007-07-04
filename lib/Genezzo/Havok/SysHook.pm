#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Havok/RCS/SysHook.pm,v 7.12 2007/02/22 09:01:41 claude Exp claude $
#
# copyright (c) 2005-2007 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Havok::SysHook;
use Genezzo::Util;
use Genezzo::Dict;

use strict;
use warnings;
use warnings::register;

use Carp;

our $VERSION;

our $Got_Hooks;       # set to 1 after all hooks get loaded
our %SysHookOriginal; # save original value of all hooks for posterity
our %ReqObjList;      # Object-Oriented Require
our %ReqObjMethod;    # Object-Oriented Meth

our $MAKEDEPS;

BEGIN {
    $VERSION = do { my @r = (q$Revision: 7.12 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

    $Got_Hooks = 0;

    my $pak1  = __PACKAGE__;
    $MAKEDEPS = {
        'NAME'     => $pak1,
        'ABSTRACT' => ' ',
        'AUTHOR'   => 'Jeffrey I Cohen (jcohen@cpan.org)',
        'LICENSE'  => 'gpl',
        'VERSION'  =>  $VERSION,
#        'UPDATED'  => Genezzo::Dict::time_iso8601()
        }; # end makedeps

    $MAKEDEPS->{'PREREQ_HAVOK'} = {
        'Genezzo::Havok' => '0.0',
        'Genezzo::Havok::Utils' => '0.0', # for userfunctions
    };

    # DML is an array, not a hash

    my $now = 
    do { my @r = (q$Date: 2007/02/22 09:01:41 $ =~ m|Date:(\s+)(\d+)/(\d+)/(\d+)(\s+)(\d+):(\d+):(\d+)|); sprintf ("%04d-%02d-%02dT%02d:%02d:%02d", $r[1],$r[2],$r[3],$r[5],$r[6],$r[7]); };

    # add_havok_pkg(modname=$pak1)

    my $dml =
        [
         "i havok 4 $pak1 SYSTEM $now 0 $VERSION"
         ];

    my %tabdefs = 
        ('sys_hook' =>  {
            create_table =>  
                'xid=n pkg=c hook=c replace=c xtype=c xname=c args=c owner=c creationdate=c version=c',
                dml => $dml
            }
         );
    $MAKEDEPS->{'TABLEDEFS'} = \%tabdefs;

    my @sql_funcs = qw(
                       add_sys_hook
                       );

    my @ins1;
    my $ccnt = 1;
    for my $pfunc (@sql_funcs)
    {
        my %attr = (module => $pak1, 
                    function => "sql_func_" . $pfunc,
                    creationdate => $now,
                    argstyle => 'HASH',
                    sqlname => $pfunc);

        my @attr_list;
        while ( my ($kk, $vv) = each (%attr))
        {
            push @attr_list, '\'' . $kk . '=' . $vv . '\'';
        }

        my $bigstr = "select add_user_function(" . join(", ", @attr_list) .
            ") from dual";
        push @ins1, $bigstr;
        $ccnt++;
    }

    # add help for all functions
    push @ins1, "select add_help(\'$pak1\') from dual";

    # XXX XXX: NOTE: check is for install, which is after create_table/dml

    $MAKEDEPS->{'DML'} = [
                          { check => [
                                      "select * from user_functions where xname = \'$pak1\'"                                      
],
                            install => \@ins1 }
                          ];

#    print Data::Dumper->Dump([$MAKEDEPS]);
}

our $GZERR = sub {
    my %args = (@_);

    return 
        unless (exists($args{msg}));

    if (exists($args{self}))
    {
        my $self = $args{self};
        if (defined($self) && exists($self->{GZERR}))
        {
            my $err_cb = $self->{GZERR};
            return &$err_cb(%args);
        }
    }

    carp $args{msg}
        if warnings::enabled();
    
};

sub MakeYML
{
    use Genezzo::Havok;

    my $makedp = $MAKEDEPS;

#    $makedp->{'UPDATED'}  = Genezzo::Dict::time_iso8601();

    return Genezzo::Havok::MakeYML($makedp);
}

# XXX XXX: Note: This method and the associated SQL script are
# deprecated, since all the work is done in HavokUse
sub MakeSQL
{
    my $bigSQL; 
    ($bigSQL = <<EOF_SQL) =~ s/^\#//gm;
#REM Copyright (c) 2005, 2006, 2007 Jeffrey I Cohen.  All rights reserved.
#REM
#REM 
#select HavokUse('Genezzo::Havok::SysHook') from dual;
#
#REM HAVOK_EXAMPLE
#i sys_hook 1 Genezzo::Dict dicthook1 Howdy_Hook require Genezzo::Havok::Examples Howdy SYSTEM TODAY 0
#i sys_hook 2 Genezzo::Dict dicthook1 Ciao_Hook  require Genezzo::Havok::Examples Ciao SYSTEM TODAY 0
#
#
#
#commit
#shutdown
#startup
EOF_SQL
    my $now = Genezzo::Dict::time_iso8601();
    $bigSQL =~ s/TODAY/$now/gm;
    $bigSQL =~ s/HAVOK_VERSION/$VERSION/gm;
    $bigSQL = "REM Generated by " . __PACKAGE__ . " version " .
        $VERSION . " on $now\nREM\n" . $bigSQL;

    return $bigSQL;
}

sub getpod
{
    my $bigHelp;
    ($bigHelp = <<'EOF_HELP') =~ s/^\#//gm;
#=head1 System_Hooks
#
#=head2  add_sys_hook : add_sys_hook(pkg=...,hook=...,module=...,function=...)
#
#To create a system hook for dicthook1 in Genezzo::Dict using the 
#function "Howdy" in Genezzo::Havok::Examples just use:
#  select add_sys_hook(
#             'pkg=Genezzo::Dict',
#             'hook=dicthook1',
#             'replace=Howdy_Hook',
#             'module=Genezzo::Havok::Examples',
#             'function=Howdy') from dual;
#
#More sophisticated functions may need to define the sys_hook
#table columns as specified in L<Genezzo::Havok::SysHook>. 
#Use the help command:
#
#  select add_sys_hook('help') from dual;
#
#to list the valid parameters
#
#
#
EOF_HELP

    my $msg = $bigHelp;

    return $msg;

} # end getpod


sub _build_sql_for_sys_hook
{
    my %required = (
                    xid => "no xid!",
                    pkg => "no package!",
                    hook => "no hook!",
                    replace => "no replace!",
                    xname => "no xname!",
                    args => "no args!"
                    );

    my $now = Genezzo::Dict::time_iso8601();

    my %optional = (
                    xtype => "require",
                    creationdate => $now,
                    owner => "SYSTEM",
                    version => 0
                    );
    my %args = (
                %optional,
		@_);

    # synonyms
    $args{xname} = $args{module} if (exists($args{module}));
    $args{args}  = $args{function} if (exists($args{function}));
    $args{pkg}  = $args{package} if (exists($args{package}));

    return undef
        unless (Validate(\%args, \%required));

    my $pattern = "\'%s\', " x 9;
    $pattern .= "\'%s\' ";

    my $bigstr = "insert into sys_hook values (" .
        sprintf($pattern,
                $args{xid},
                $args{pkg},
                $args{hook},
                $args{replace},
                $args{xtype},
                $args{xname},
                $args{args},

                $args{owner},
                $args{creationdate},
                $args{version});

    $bigstr .= ")";

    return $bigstr;
}

sub sql_func_add_sys_hook
{
    my %args= @_;

    my $dict = $args{dict};
    my $dbh  = $args{dbh};
    my $fn_args = $args{function_args};
    
#    print Data::Dumper->Dump($fn_args);

    my $now = Genezzo::Dict::time_iso8601();

    # list the optional values
    my %nargs = (
                 xtype => "require",
                 creationdate => $now,
                 owner => "SYSTEM",
                 version => 0,

                 dict => $dict
                 );

    my $do_help = 0;

    $do_help = 1 unless (scalar(@{$fn_args}));
    
    my $valid = 
        'xid|pkg|hook|replace|xtype|xname|args|owner|creationdate|version';

    $valid .= '|module|function|package'; # additional synonyms

    for my $argi (@{$fn_args})
    {
        # separate key=val pairs into hash args

        my @foo;
        @foo = ($argi =~ m/^(\s*\w+\s*\=\s*)(.*)(\s*)$/)
            if ($argi =~ m/\w+\s*\=/);


        if ($argi =~ m/^\s*($valid)\s*\=/i)
        {
            my $nargtype = $foo[0];
            # remove the spaces and equals ("=");
            $nargtype =~ s/\s//g;
            $nargtype =~ s/\=//g;

            $nargtype = 'xname' if ($nargtype =~ m/^module/i);
            $nargtype = 'args' if ($nargtype =~ m/^function/i);
            $nargtype = 'pkg' if ($nargtype =~ m/^package/i);

            $nargs{lc($nargtype)} = $foo[1];
        }
        else
        {
            if (scalar(@{$fn_args}) == 1)
            {
                if ($argi =~ m/^help$/i)
                {
                    $do_help = 1;
                    last;
                }
            } # end if 1 arg
        }
    } # end for

    if ($do_help)
    {
        my $outi = "Valid arguments are:\n    ";

        $outi .= join(" ",split(/\|/, $valid)) . "\n";

        my $bigexample;
        ($bigexample = <<EOF_EXAMPLE) =~ s/^\#//gm;
#
#To create a system hook for dicthook1 in Genezzo::Dict using the 
#function "Howdy" in Genezzo::Havok::Examples just use:
#  select add_sys_hook(
#             'pkg=Genezzo::Dict',
#             'hook=dicthook1',
#             'replace=Howdy_Hook',
#             'module=Genezzo::Havok::Examples',
#             'function=Howdy') from dual;
EOF_EXAMPLE

        $outi .= $bigexample;
        
        return $outi;
    }

    unless (exists($nargs{xid}))
    {
        my $hashi  = $dict->DictTableGetTable (tname => "sys_hook") ;
        my $tv = tied(%{$hashi});

        $nargs{xid} = $dict->DictGetNextVal(tname => "sys_hook",
                                            col   => "xid",
                                            tieval => $tv);

    }

    my $bigstr = _build_sql_for_sys_hook(%nargs);

    return 0 unless(defined($bigstr));

    my $sth =
        $dbh->prepare($bigstr);
    
    return 0
        unless ($sth);


    # insert the function definition in the user_function table
    return 0
        unless ($sth->execute());

    # load the hook
    return Genezzo::Havok::SysHook::LoadSysHook(%nargs);

}

sub LoadSysHook
{
    my %optional;

    my %required = (
                    xid          => "no xid!",
                    xtype        => "no xtype!",
                    xname        => "no xname!",
                    owner        => "no owner!",
                    creationdate => "no creationdate!",
                    args         => "no args!",

                    pkg          => "no pkg!",
                    hook         => "no hook!",
                    replace      => "no replace!",

                    dict         => "no dictionary!"
                    );

    my %args = (%optional,
                @_);

    return 0
        unless (Validate(\%args, \%required));

    my $xid    = $args{xid};
    my $xtype  = $args{xtype};
    my $xname  = $args{xname};
    my $owner  = $args{owner};
    my $dat    = $args{creationdate};
    my $xargs  = $args{args};

    my $xpkg   = $args{pkg};
    my $hook   = $args{hook};
    my $repl   = $args{replace};

    my $dict   = $args{dict};

    my $stat = 1;

    my $save_previous_hook;

    # block 1
    {
        my $mainf = $xpkg . "::"  . $hook;

        my @varlist; # list of variables to hold previous value of coderef

        if (defined($repl)
            && length($repl))
        {
                
            # build name of variable to hold previous value of
            # hook coderef
            
            $repl = $xname . "::" . $repl; # scope for require package
            push @varlist, $repl;
        }

        # have we seen this hook before?
        unless (exists($SysHookOriginal{"$mainf"}))
        {

            # create a placeholder, even for non-existant
            # functions.  Then we know to "undef" them if we
            # re-initialize...
            
            $SysHookOriginal{"$mainf"} = undef;

            # save the original value for a hook variable if necessary
            my $orig_var = 'SysHookOriginal{"'. $mainf . '"}';
            push @varlist, $orig_var;
        }

        if (scalar(@varlist))
        {
            # we have a hook that needs saving...
            $save_previous_hook = "";

            # save to modules "replace" var and SysHookOriginal 
            # if necessary
            for my $varname (@varlist)
            {
                $save_previous_hook .= '$' . $varname . ' = \&' . $mainf .
                    ' if defined(&' . $mainf . ');';
            }
            greet $save_previous_hook;

        }
        else
        {
            # do nothing
            $save_previous_hook = undef;
        }

    } # end block 1

    if ($xtype =~ m/^(oo_require|require)$/i)
    {
        my $req_str = "require $xname";

        eval "$save_previous_hook"
            if (defined($save_previous_hook));
            
        greet $req_str;

        unless (eval $req_str)
        {
            my %earg = (#self => $self,
                        msg => "no such package - $xname - for table sys_hook, row $xid");

            &$GZERR(%earg)
                if (defined($GZERR));

#                next;
            return 0;
        }


        # XXX XXX: check for existance of "args" function...

        no strict 'refs';
        no warnings 'redefine';

        my @inargs;

        if ($xargs =~ m/\s/)
        {
            @inargs = split(/\s/, $xargs);
        }
        else
        {
            push @inargs, $xargs;
        }

        if ($xtype =~ m/^(oo_require)$/i)
        {
            # Object-Oriented Require
            unless (exists($ReqObjList{"$xname"}))
            {
                whisper "init object for package $xname";
                
                my $obj;
                my $initstr = '$obj = ' . $xname ;
                $initstr   .= "->" . 'SysHookInit($dict)';
                whisper "$initstr";
                eval " $initstr " ;
                if ($@)
                {
                    my %earg = (#self => $self,
                                msg => "$@\nbad pkg init : $initstr");
                    
                    &$GZERR(%earg)
                        if (defined($GZERR));

                    $stat = 0;
                }

                # create an entry even if the init fails
                $ReqObjList{"$xname"} = $obj;                        
            }
                
        }

        my $obj1;

        for my $fname (@inargs)
        {
            # Note: add functions to specified namespace...

            my $mainf = $xpkg . "::"  . $hook;
            my $packf =  $xname . "::" . $fname;

            my $func = "sub " . $mainf ;
            if (($xtype =~ m/^(oo_require)$/i) &&
                exists($ReqObjList{"$xname"}) &&
                defined($ReqObjList{"$xname"}))
            {
                $obj1 = $ReqObjList{"$xname"};
                #$ReqObjMethod{$packf} = sub { $obj1->$packf(@_) };
                #$func .= '{ return $ReqObjMethod{' . $packf . '}->(@_); }';
#                    $func .= '{ my $mref = sub { $obj1->$packf(@_) };';
#                    $func .= ' return $mref->(@_); }';

                # lots of work to avoid 'Variable "$mref" may be
                # unavailable...'

                $func .= '{ my $mref = ' .
                    'sub { $Genezzo::Havok::SysHook::ReqObjList{"' 
                    . $xname .'"}->' . $packf . '(@_) };';
                $func .= ' return $mref->(@_); }';

#                    $mref = sub { $obj1->$packf(@_) };
#                    $func .= '{ return $mref->(@_); }';
            }
            else
            {
                $func .= "{ return " . $packf . '(@_); }';
            }            

            whisper $func;

#            eval {$func } ;
            eval " $func " ;
            if ($@)
            {
                my %earg = (#self => $self,
                            msg => "$@\nbad function : $func");

                &$GZERR(%earg)
                    if (defined($GZERR));

                $stat = 0;
            }

        }

            
    } # end  if $xtype =~ (oo_require|require)
    elsif ($xtype =~ m/^function$/i)
    {
        my $doublecolon = "::";


        # XXX XXX: what about hook name? what should it mean?

        unless ($xname =~ m/$doublecolon/)
        {
            # Note: add functions to  namespace...
            
            $xname = $xpkg . "::" . $xname;
        }

        my $func = "sub " . $xname . " " . $xargs;
            
#            whisper $func;

#            eval {$func } ;
        eval " $func " ;
        if ($@)
        {
            my %earg = (#self => $self,
                        msg => "$@\nbad function : $func");
            
            &$GZERR(%earg)
                if (defined($GZERR));
            $stat = 0;
        }
    } # end if xtpe =~ function
    else
    {
        my %earg = (#self => $self,
                    msg => "unknown user extension - $xtype");

        &$GZERR(%earg)
            if (defined($GZERR));

        $stat = 0;
    }

    return $stat;

} # end loadsyshook

sub HavokInit
{
#    whoami;
    my %optional = (phase => "init");
    my %required = (dict  => "no dictionary!",
                    flag  => "no flag"
                    );

    my %args = (%optional,
		@_);
#		
    my @stat;

    push @stat, 0, $args{flag};
#    whoami (%args);

    return @stat
        unless (Validate(\%args, \%required));

    if ($Got_Hooks)
    {

        # XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX 
        # don't load hooks twice to avoid circular links!!  Is it
        # sufficient to call the first entry for each hook and reset
        # the hook to its "replace" var, i.e for dicthook1, set
        # dicthook1 = &Howdy_Hook ? Or use SysHookOriginal hash?
        # XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX 
        $stat[0] = 1; # ok!
        return @stat;
    }

    my $dict   = $args{dict};
    my $phase  = $args{phase};

    return @stat
        unless ($dict->DictTableExists(tname => "sys_hook",
                                       silent_notexists => 1));

    my $hashi  = $dict->DictTableGetTable (tname => "sys_hook") ;

    return @stat # no User Extensions
        unless (defined ($hashi));

    my $tv = tied(%{$hashi});

    while ( my ($kk, $vv) = each ( %{$hashi}))
    {
        my $getcol = $dict->_get_col_hash("sys_hook");  
        my $xid    = $vv->[$getcol->{xid}];
        my $xtype  = $vv->[$getcol->{xtype}];
        my $xname  = $vv->[$getcol->{xname}];
        my $owner  = $vv->[$getcol->{owner}];
        my $dat    = $vv->[$getcol->{creationdate}];
        my $xargs  = $vv->[$getcol->{args}];

        my $xpkg   = $vv->[$getcol->{pkg}];
        my $hook   = $vv->[$getcol->{hook}];
        my $repl   = $vv->[$getcol->{replace}];
            
#        greet $vv;
        
        my $lstat =
            LoadSysHook(
                        xid          => $xid,
                        xtype        => $xtype,
                        xname        => $xname,
                        owner        => $owner,
                        creationdate => $dat,
                        args         => $xargs,

                        pkg          => $xpkg,
                        hook         => $hook,
                        replace      => $repl,

                        dict         => $dict
                        );

    } # end while

    $Got_Hooks = 1;

    greet %SysHookOriginal;

    $stat[0] = 1; # ok!
    return @stat;
}

sub HavokCleanup
{
#    whoami;
    return HavokInit(@_, phase => "cleanup");
}


END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Havok::SysHook - load the SysHook table

=head1 SYNOPSIS

 # don't say "use Genezzo::Havok::SysHook".  Update the
 # dictionary havok table:

select HavokUse('Genezzo::Havok::SysHook') from dual;


=head1 DESCRIPTION

Basic Havok module - load the SysHook table

create table sys_hook (
    xid   number,
    pkg   char,
    hook  char,
    replace  char,
    xtype char,
    xname char,
    args  char,
    owner char, 
    creationdate char,
    version char
    );

=over 4

=item xid - a unique id number
  
=item pkg - name of package for this hook

=item hook - name of hook function

=item replace - unique name for previous hook coderef.  
If blank or null, just replace existing hook, 
otherwise is variable name for
previous version of the hook, and may get called from new hook

=item  xtype - the string "require" or "function"

=item xname - if xtype = "require", then xname is a package name, like
"Text::Soundex".  if xtype = "function", xname is a function name.  A
function name may be qualified with a package.


=item args - if xtype = "require", an (optional) blank-separated list
of functions to import to the default Genezzo namespace.  if xtype =
"function", supply an actual function body in curly braces.

=item owner - owner of the package or function

=item creationdate - date row was created

=back

=head2 Example:

insert into sys_hook values (1, 'Genezzo::Dict', 'dicthook1', 'Howdy_Hook',
'require', 'Genezzo::Havok::Examples',  
'Howdy', 'SYSTEM', '2004-09-21T12:12');

The row causes SysHook to "require Genezzo::Havok::Examples", and
calls the "Howdy" function from the hook function "dicthook1" in the
package Genezzo::Dict.  The previous coderef for the function "dicthook1"
(if it exists) is assigned to $Genezzo::Havok::Examples::Howdy_Hook. 
The Howdy function can call &$Howdy_Hook() to activate the original
"dicthook1" function.


=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=back

=head1 RISKS 

Replacing system functions in an operational database has
approximately the same level of risk exposure as running with the
bulls at Pamplona with your pants around your ankles.  Which is to
say, "somewhat foolhardy".  

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS

=head1 TODO

=over 4

=item should be able to dynamically create hook vars, versus using
existing "our" vars.

=item should we do something smart on dictionary shutdown, like unload hooks?  Or have a clever way to re-init and reload a hook?

=back

=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>.

Copyright (c) 2005-2007 Jeffrey I Cohen.  All rights reserved.

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

Address bug reports and comments to: jcohen@genezzo.com

For more information, please visit the Genezzo homepage 
at L<http://www.genezzo.com>

=cut
