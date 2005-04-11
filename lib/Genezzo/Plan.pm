#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/RCS/Plan.pm,v 1.5 2005/04/09 06:16:01 claude Exp claude $
#
# copyright (c) 2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Plan;
use Genezzo::Util;

use Genezzo::Plan::TypeCheck;
use Genezzo::Plan::MakeAlgebra;
use Genezzo::Parse::SQL;
use Parse::RecDescent;

use strict;
use warnings;
use warnings::register;

use Carp;

our $VERSION;

BEGIN {
    $VERSION = do { my @r = (q$Revision: 1.5 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

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

our $GZERR_MAGIC; # refer back to gzerr in a magic way...

sub _init
{
    my $self = shift;

    $self->{dictobj}    = undef; # nothing
    $self->{parser}     = Genezzo::Parse::SQL->new();
    $self->{getAlgebra} = Genezzo::Plan::MakeAlgebra->new();

    my %nargs = @_;
    $nargs{plan_ctx}    = $self; # add self to args list;

    $self->{typeCheck}  = Genezzo::Plan::TypeCheck->new(%nargs);

    # Be stunned and amazed at the power of Perl!
    # Supply a hook to the parser to reroute its error reporting thru GZERR
    # Is there a simpler way to do this?  I hope so.

    # create a closure referring to caller's self and gzerr
    $GZERR_MAGIC =
        sub {
            my $msg1 = shift;
            my %earg = (self => $self, 
                        msg =>  $msg1, 
                        severity => 'error');

            &$GZERR(%earg)
                if (defined($GZERR));
        };

    # we don't need to redefine this function if it already exists
    # (and we'd like to avoid a compiler warning)
    unless (defined(&Parse::RecDescent::Genezzo::Parse::SQL::gnz_err_hook))
    {
        my $func;
        ($func = <<'EOF_func') =~ s/^\#//gm;
#
#        # SQLGrammar supports a hook in the start rule to override the
#        # default error reporting mechanism
#        
#        # create the gnz_err_hook in the correct namespace
#        sub Parse::RecDescent::Genezzo::Parse::SQL::gnz_err_hook
#        {
#            my $msg = shift; 
#            &$Genezzo::Plan::GZERR_MAGIC($msg);
#        }
#
EOF_func

    # hope this works!
    eval " $func ";

        if ($@)
        {
            my %earg = (self => $self,
                        msg => "$@\nbad function : $func");
            
            &$GZERR(%earg)
                if (defined($GZERR));
            
            return 0;
        }
    }
    
    return 1;
}

sub new 
{
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
    my $self = { };
    
    my %args = (@_);

    if ((exists($args{GZERR}))
        && (defined($args{GZERR}))
        && (length($args{GZERR})))
    {
        # NOTE: don't supply our GZERR here - will get
        # recursive failure...
        $self->{GZERR} = $args{GZERR};
        my $err_cb     = $self->{GZERR};
        # capture all standard error messages
        $Genezzo::Util::UTIL_EPRINT = 
            sub {
                &$err_cb(self     => $self,
                         severity => 'error',
                         msg      => @_); };
        
        $Genezzo::Util::WHISPER_PRINT = 
            sub {
                &$err_cb(self     => $self,
#                         severity => 'error',
                         msg      => @_); };
    }

    return undef
        unless (_init($self, %args));

    return bless $self, $class;

} # end new

# get or set the dictionary object
sub Dict
{
    my $self = shift;

    if (scalar(@_))
    {
        $self->{dictobj} = shift;
    }
    return $self->{dictobj};    
}


sub Parse
{
    my $self = shift;

    my %required = (
                    statement => "no statement!"
                    );

    my %args = ( # %optional,
                @_);

    return undef
        unless (Validate(\%args, \%required));

    return ($self->{parser}->sql_000($args{statement}));

}

sub Algebra
{
    my $self = shift;

    my %args = ( # %optional,
                @_);

    my $parse_tree;

    if (exists($args{statement}))
    {
        $parse_tree = $self->Parse(statement => $args{statement});
    }

    unless (defined($parse_tree))
    {
        unless (exists($args{parse_tree}))
        {
            my $msg = "no parse tree or statement";
            my %earg = (self => $self, msg => $msg,
                        severity => 'warn');
            
            &$GZERR(%earg)
                if (defined($GZERR));

            return 0;
        }

        $parse_tree = $args{parse_tree};
    }

    return $self->{getAlgebra}->Convert(parse_tree => $parse_tree);

}

sub TypeCheck
{
    my $self = shift;

    my %required = (
                    algebra   => "no algebra !",
                    statement => "no sql statement !"
                    );

    my %args = ( # %optional,
                @_);

    return undef
        unless (Validate(\%args, \%required));

    return $self->{typeCheck}->TypeCheck(algebra   => $args{algebra},
                                         statement => $args{statement},
                                         dict      => $self->Dict());

}

sub GetFromWhereEtc
{
    my $self = shift;

    my %required = (
                    algebra   => "no algebra !"
                    );

    my %args = ( # %optional,
                @_);

    return undef
        unless (Validate(\%args, \%required));

    return $self->{typeCheck}->GetFromWhereEtc(algebra   => $args{algebra},
                                               dict      => $self->Dict());

}


END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Plan - Parsing, Planning and Execution

=head1 SYNOPSIS

use Genezzo::Plan;


=head1 DESCRIPTION



=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item Parse

Parse a SQL statement and return a parse tree.

=item Algebra

Take a SQL statement or parse tree and return the corresponding
relational algebra.

=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS


=head1 TODO

=over 4

=item update pod

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
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

Address bug reports and comments to: jcohen@genezzo.com

For more information, please visit the Genezzo homepage 
at L<http://www.genezzo.com>

=cut
