#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Plan/RCS/MakeAlgebra.pm,v 1.2 2005/03/19 08:48:47 claude Exp claude $
#
# copyright (c) 2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Plan::MakeAlgebra;
use Genezzo::Util;

use strict;
use warnings;
use warnings::register;

use Carp;

our $VERSION;

BEGIN {
    $VERSION = do { my @r = (q$Revision: 1.2 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

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


sub new 
{
#    whoami;
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

#    return undef
#        unless (_init($self, %args));

    return bless $self, $class;

} # end new

sub Convert # public
{
#    whoami;
    my $self = shift;
    my %required = (
                    parse_tree => "no parse tree!"
                    );

    my %args = ( # %optional,
                @_);

    return 0
        unless (Validate(\%args, \%required));

    my $parse_tree = $args{parse_tree};

    return convert_algebra($parse_tree);
}

# recursive function to convert parse tree to relational algebra
sub convert_algebra # private
{
#    whoami;
    my $sql = shift;

    # recursively convert all elements of array
    if (ref($sql) eq 'ARRAY')
    {
        my $maxi = scalar(@{$sql});
        $maxi--;
        for my $i (0..$maxi)
        {
            $sql->[$i] = convert_algebra($sql->[$i]);
        }

    }
    if (ref($sql) eq 'HASH')
    {
        keys( %{$sql} ); # XXX XXX: need to reset expression!!
        # recursively convert all elements of hash, but treat
        # sql_select specially

        while ( my ($kk, $vv) = each ( %{$sql})) # big while
        {
            if ($kk !~ m/^sql_select$/)
            {
                $sql->{$kk} = convert_algebra($vv);
            }
            else
            {
                
                # convert SQL SELECT to a basic relational algebra,
                # (PROJECT ( FILTER (THETA-JOIN)))
                #
                # First, perform a theta-join of the FROM clause
                # tables.
                #
                # Next, filter out the rows that don't satisfy the
                # WHERE clause.
                #
                # Perform grouping and filter the results of the
                # HAVING clause.
                #
                # Finally, project the required SELECT list entries as
                # output.

                my @op_list = qw(
                                 theta_join    from_clause
                                 filter        where_clause
                                 alg_group     groupby_clause
                                 hav           having_clause
                                 project       select_list
                                 );

                # build a list of each relational algebra operation and
                # the associated SQL statement clause.
                my %alg_oper_map = @op_list;
                my %alg_oper;

                while (my ($operkey, $operval) = each (%alg_oper_map))
                {
                    # associate each operation with its operands.
                    # Note that some operands might contain SQL
                    # SELECTs, so process them recursively with
                    # convert_algebra.

                    $alg_oper{$operkey} = convert_algebra($vv->{$operval});
                }

                # build a nested hash of relational algebra
                # operations, starting with theta-join as the
                # innermost operator.
                #
                # The simplest output is just a degenerate
                # theta-join of a single table.
                # The most complex output is a 
                # (project (filter(groupby(filter(theta-join)))))
                # 
                # More complicated combinations arise from compound
                # statements using set operations (UNION, INTERSECT)
                # or subqueries.

                my $hashi;
                my $prev;
                my $toggle = -1; # get every other entry, 
                                 # ie get the hash keys in order
              L_all_opers:
                for my $oper (@op_list)
                {
                    $toggle *= -1;
                    next L_all_opers if ($toggle) < 1;
                    
                    if ($oper eq "theta_join")
                    {
                        # build first (innermost) hash entry
                        # 
                        # theta_join => 
                        #   from_clause => converted(from_clause)
                        # 
                        
                        $hashi = 
                        {
                            $oper => {
                                $alg_oper_map{$oper} => $alg_oper{$oper}
                            }
                        };
                        $prev = $oper;
                    }
                    else
                    {
                        # cleanup the tree: use more consistent names
                        # for operators and operands.
                        my $oper_alias   = $oper;
                        my $operands_key = $alg_oper_map{$oper};

                        if ($oper eq "filter")
                        {
                            # use generic search_cond, vs where_clause
                            $operands_key = "search_cond";

                            # ignore empty list
                            my $a1 = $alg_oper{$oper};
                            if ((ref($a1) eq 'ARRAY')
                                && (0 == scalar(@{$a1})))
                            {
                                next L_all_opers;
                            }
                        }
                        elsif ($oper eq "alg_group")
                        {
                            # ignore empty list
                            my $a1 = $alg_oper{$oper};
                            if ((ref($a1) eq 'ARRAY')
                                && (0 == scalar(@{$a1})))
                            {
                                next L_all_opers;
                            }
                        }
                        elsif ($oper eq "hav")
                        {
                            # having is just a filter, and having clause
                            # is just a search condition
                            $oper_alias   = "filter";
                            $operands_key = "search_cond";

                            # ignore empty list
                            my $a1 = $alg_oper{$oper};
                            if ((ref($a1) eq 'ARRAY')
                                && (0 == scalar(@{$a1})))
                            {
                                next L_all_opers;
                            }

                        }
                        elsif ($oper eq "project")
                        {
                            # if performing a "SELECT * FROM..." then
                            # project is superfluous

                            if (
                                   (exists($vv->{all_distinct}))
                                && (ref($vv->{all_distinct}) eq 'ARRAY')
                                && (0 == scalar(@{$vv->{all_distinct}})))
                            {
                                # XXX XXX XXX XXX XXX XXX XXX XXX 
                                # XXX XXX XXX XXX XXX XXX XXX XXX 
                                # Keep the project for "select *"
                                # because the optimizer might rewrite
                                # the query and add extra tables to
                                # the FROM list.  Expand the "*" to
                                # only include the columns from the
                                # original FROM list.
                                # XXX XXX XXX XXX XXX XXX XXX XXX 
                                # XXX XXX XXX XXX XXX XXX XXX XXX 
                                if (0 &&
                                    exists($vv->{select_list})
                                    && (!ref($vv->{select_list}))
                                    && ($vv->{select_list} eq 'STAR'))
                                {
                                    next L_all_opers;
                                }
                            }
                        }

                        # build new hash, wrapping previous.
                        # e.g., if had theta-join, and outer oper
                        # is a filter, we get:
                        # filter => (search_cond => ...
                        #            theta_join => (...)
                        #           )

                        $hashi = 
                        {
                            $oper_alias => {
                                $operands_key => $alg_oper{$oper},
                                $prev         => $hashi->{$prev}
                            }
                        };
                        if ($oper eq "project")
                        {
                            # project has additional all/distinct attribute

                            $hashi->{$oper}->{all_distinct} =
                                $vv->{all_distinct};
                        }

                        $prev = $oper_alias;
                        
                    }
                } # end for oper list

                $sql->{$kk} = $hashi;

            } # end else
        } # end big while
    }

    return $sql;
} # end convert_algebra


END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Plan::MakeAlgebra - Convert a SQL parse tree to relational algebra

=head1 SYNOPSIS

use Genezzo::Plan::MakeAlgebra;


=head1 DESCRIPTION


=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item Convert

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
