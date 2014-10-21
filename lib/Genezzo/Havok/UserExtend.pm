#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Havok/RCS/UserExtend.pm,v 1.10 2005/02/02 06:43:24 claude Exp claude $
#
# copyright (c) 2004, 2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Havok::UserExtend;
use Genezzo::Util;

use strict;
use warnings;
use warnings::register;

use Carp;

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

    my $dict   = $args{dict};
    my $phase  = $args{phase};

    return @stat
        unless ($dict->DictTableExists(tname => "user_extend",
                                       silent_notexists => 1));

    my $hashi  = $dict->DictTableGetTable (tname => "user_extend") ;

    return @stat # no User Extensions
        unless (defined ($hashi));

    my $tv = tied(%{$hashi});

    while ( my ($kk, $vv) = each ( %{$hashi}))
    {
        my $getcol = $dict->_get_col_hash("user_extend");  
        my $xid    = $vv->[$getcol->{xid}];
        my $xtype  = $vv->[$getcol->{xtype}];
        my $xname  = $vv->[$getcol->{xname}];
        my $owner  = $vv->[$getcol->{owner}];
        my $dat    = $vv->[$getcol->{creationdate}];
        my $xargs  = $vv->[$getcol->{args}];

#        greet $vv;

        if ($xtype =~ m/^require$/i)
        {
            unless (eval "require $xname")
            {
                my %earg = (#self => $self,
                            msg => "no such package - $xname - for table user_extend, row $xid");

                &$GZERR(%earg)
                    if (defined($GZERR));

                next;
            }

            no strict 'refs';

            my @inargs;

            if ($xargs =~ m/\s/)
            {
                @inargs = split(/\s/, $xargs);
            }
            else
            {
                push @inargs, $xargs;
            }


            for my $fname (@inargs)
            {
                # Note: add functions to "main" namespace...

                my $mainf = "Genezzo::GenDBI::" . $fname;
                my $packf =  $xname . "::" . $fname;

                my $func = "sub " . $mainf ;
                $func .= "{ " . $packf . '(@_); }';
            
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

            
        }
        elsif ($xtype =~ m/^function$/i)
        {
            my $doublecolon = "::";

            unless ($xname =~ m/$doublecolon/)
            {
                # Note: add functions to "main" namespace...

                $xname = "Genezzo::GenDBI::" . $xname;
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

Genezzo::Havok::UserExtend - load the UserExtend table

=head1 SYNOPSIS

 # don't say "use Genezzo::Havok::UserExtend".  Update the
 # dictionary havok table:

insert into havok values (1, "Genezzo::Havok::UserExtend", "SYSTEM", 
"2004-09-21T12:12", 0);


=head1 DESCRIPTION

Basic Havok module - load the UserExtend table

create table user_extend (
    xid   number,
    xtype char,
    xname char,
    args  char,
    owner char, 
    creationdate char,
    version char
    );

=over 4

=item xid - a unique id number
  

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

insert into user_extend values (1, "require", "Genezzo::Havok::Examples",  
"isRedGreen", "SYSTEM", "2004-09-21T12:12");

The row causes UserExtend to "require Genezzo::Havok::Examples", and
it imports "isRedGreen" into the default Genezzo namespace* (actually,
it creates a stub function that calls
Genezzo::Havok::Examples::isRedGreen").



=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item  isRedGreen

=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS

=head1 TODO

=over 4

=item Need to fix "import" mechanism so can load specific functions
into Genezzo::GenDBI namespace, versus creating stub functions.
Use "import" and "export_to_level".

=item Could just load Acme::Everything and we'd be done...

=back

=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>.

Copyright (c) 2004, 2005 Jeffrey I Cohen.  All rights reserved.

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
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

Address bug reports and comments to: jcohen@genezzo.com

For more information, please visit the Genezzo homepage 
at L<http://www.genezzo.com>

=cut
