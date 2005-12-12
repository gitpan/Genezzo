#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Havok/RCS/SysHook.pm,v 7.4 2005/12/12 09:14:17 claude Exp claude $
#
# copyright (c) 2005 Jeffrey I Cohen, all rights reserved, worldwide
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

BEGIN {
    $VERSION = do { my @r = (q$Revision: 7.4 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

    $Got_Hooks = 0;
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

sub MakeSQL
{
    my $bigSQL; 
    ($bigSQL = <<EOF_SQL) =~ s/^\#//gm;
#REM Copyright (c) 2005 Jeffrey I Cohen.  All rights reserved.
#REM
#REM 
#ct sys_hook xid=n pkg=c hook=c replace=c xtype=c xname=c args=c owner=c creationdate=c version=c
#i havok 3 Genezzo::Havok::SysHook SYSTEM TODAY 0 HAVOK_VERSION
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

        my $save_previous_hook;

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

        }

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

                next;
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
                }

            }

            
        }
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
            }
        }
        else
        {
            my %earg = (#self => $self,
                        msg => "unknown user extension - $xtype");

            &$GZERR(%earg)
                if (defined($GZERR));
        }

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

insert into havok values (3, 'Genezzo::Havok::SysHook', 'SYSTEM', 
'2004-09-21T12:12', 0);


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

Copyright (c) 2005 Jeffrey I Cohen.  All rights reserved.

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
