#!/usr/bin/perl
#
# $Header: /Users/claude/g3/lib/Genezzo/BufCa/RCS/BufCaElt.pm,v 6.1 2004/08/12 09:31:15 claude Exp claude $
#
# copyright (c) 2003, 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
use strict;
use warnings;

package Genezzo::BufCa::BufCaElt;

use Genezzo::Util;
use Carp;
use warnings::register;

use Genezzo::BufCa::DirtyScalar;

BEGIN {
    use Exporter   ();
    our ($VERSION, @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);

    # set the version for version checking
#    $VERSION     = 1.00;
    # if using RCS/CVS, this may be preferred
    $VERSION = do { my @r = (q$Revision: 6.1 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

    @ISA         = qw(Exporter);
#    @EXPORT      = qw(&func1 &func2 &func4 &func5);
    @EXPORT      = ( );
    %EXPORT_TAGS = ( );     # eg: TAG => [ qw!name1 name2! ],

    # your exported package globals go here,
    # as well as any optionally exported functions
#    @EXPORT_OK   = qw($Var1 %Hashit &func3 &func5);
    @EXPORT_OK   = ( );

}

our @EXPORT_OK;

# non-exported package globals go here


# initialize package globals, first exported ones
#my $Var1   = '';
#my %Hashit = ();

# then the others (which are still accessible as $Some::Module::stuff)
#$stuff  = '';
#@more   = ();

# all file-scoped lexicals must be created before
# the functions below that use them.

# file-private lexicals go here
#my $priv_var    = '';
#my %secret_hash = ();
# here's a file-private function as a closure,
# callable as &$priv_func;  it cannot be prototyped.
#my $priv_func = sub {
    # stuff goes here.
#};

# make all your functions, whether exported or not;
# remember to put something interesting in the {} stubs
#sub func1      {print "hi";}    # no prototype
#sub func2()    {}    # proto'd void
#sub func3($$)  {}    # proto'd to 2 scalars
#sub func5      {print "ho";}    # no prototype

sub _init
{
    #whoami;
    #greet @_;
    my $self = shift;

    my %required = (
                    blocksize => "no blocksize !"
                    );

    my %args = (
                @_);

    return 0
        unless (Validate(\%args, \%required));

    # XXX: a bit redundant to keep blocksize for each bce - should be
    # constant for entire cache...
    $self->{blocksize} = $args{blocksize};

    my $buf;
    $self->{tbuf}  = tie $buf, "Genezzo::BufCa::DirtyScalar";

    $buf = "\0" x $self->{blocksize};
    $self->{bigbuf} = \$buf;

    $self->{pin}    = 0;
    $self->{dirty}  = 0;

    return 1;
}

sub new 
{
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
    my $self = { };

    my %args = (@_);

    return undef
        unless (_init($self,%args));

    my $foo = bless $self, $class;
    $self->_postinit();
    return $foo;

} # end new

sub _postinit
{
    my $self = shift;

    # supply a closure so the bce is marked dirty
    # if the underlying tied buffer gets overwritten
    my $foo = sub { $self->_dirty(1); };
    $self->{tbuf}->_StoreCB($foo);

}

sub _pin
{
# XXX: need atomic increment/decrement

    my $self = shift;

    if (scalar(@_))
    {
        my $pin_inc = shift;
#        whisper "pinning $pin_inc -> ";
        $self->{pin} += $pin_inc;
    }

    # XXX XXX XXX XXX: pin > 1 possible -- block zero (file header)
    # gets pinned multiple times

#    whisper "current pin val: ", $self->{pin};
    return $self->{pin};

} 

sub _dirty
{
    my $self = shift;
    $self->{dirty} = shift if @_ ;

    return $self->{dirty};

} 


END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE


1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 ARGUMENTS

=head1 FUNCTIONS

=head2 EXPORT

=head1 LIMITATIONS

various

=head1 #TODO

=over 4

=back

=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>.

Copyright (c) 2003, 2004 Jeffrey I Cohen.  All rights reserved.

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

=cut
