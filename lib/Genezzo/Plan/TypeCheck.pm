#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Plan/RCS/TypeCheck.pm,v 1.15 2005/05/10 09:09:27 claude Exp claude $
#
# copyright (c) 2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Plan::TypeCheck;
use Genezzo::Util;

use strict;
use warnings;
use warnings::register;

use Carp;

our $VERSION;

BEGIN {
    $VERSION = do { my @r = (q$Revision: 1.15 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

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

sub _init
{
    my $self = shift;
    my %args = @_;

    return 0
        unless (exists($args{plan_ctx})
                && defined($args{plan_ctx}));

    $self->{plan_ctx} = $args{plan_ctx};

    return 1;

}


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

    return undef
        unless (_init($self, %args));

    return bless $self, $class;

} # end new

sub TypeCheck
{
    my $self = shift;
    
    my %required = (
                    algebra   => "no algebra !",
                    statement => "no sql statement !",
                    dict      => "no dictionary !"
                    );
    
    my %args = ( # %optional,
                 @_);
    
    return undef
        unless (Validate(\%args, \%required));

    my $algebra = $args{algebra};

    my $err_status;

    $algebra = $self->TableCheck(algebra => $algebra,
                                 dict    => $args{dict}
                                 );

    return ($algebra, 1)
        unless (defined($algebra)); # if error

    unless (scalar(@{$self->{tc1}->{tc_err}->{nosuch_table}}))
    {
        $algebra = $self->ColumnCheck(algebra   => $algebra,
                                      dict      => $args{dict},
                                      statement => $args{statement}
                                      );
    }

    unless (exists($self->{tc1}) &&
            exists($self->{tc2}) &&
            exists($self->{tc3}))
    {
        greet "incomplete tc";
        $err_status = 1;
    }

    if (!defined($err_status))
    {
        if (
               scalar(@{$self->{tc1}->{tc_err}->{nosuch_table}})
            || scalar(@{$self->{tc3}->{tc_err}->{nosuch_column}})
            )
        {
            greet "tc errors";
            $err_status = 1;
        }
    }
    

    return ($algebra, $err_status);
}

sub TableCheck
{
    my $self = shift;
    
    my %required = (
                    algebra => "no algebra !",
                    dict    => "no dictionary !"
                    );
    
    my %args = ( # %optional,
                 @_);
    
    return undef
        unless (Validate(\%args, \%required));
    
    my $algebra = $args{algebra};

    # XXX XXX: maybe break the type check phases into separate packages

    # first, fetch table info from dictionary

    my $tc1 = {}; # type check tree context for tree walker
    $self->{tc1} = $tc1;

    # local tree walk state
    $tc1->{tpos} = 0; # mark each table

    # save bad tables for error reporting...
    $tc1->{tc_err}->{nosuch_table} = [];
    $algebra = $self->_get_table_info($algebra, $args{dict});

    # next, cross reference table info with query blocks

    my $tc2 = {}; # type check tree context for tree walker
    $self->{tc2} = $tc2;

    # local tree walk state
    $tc2->{qb_list} = []; # build an arr starting with current query block num
    $tc2->{qb_dependency} = []; # save qb parent dependency

    # save table definition/query block info for later type check phases...
    $tc2->{tablist} = []; # arr by qb num of table information
    $algebra = $self->_check_table_info($algebra, $args{dict});

    if (0)
    {
        local $Data::Dumper::Indent   = 1;
        local $Data::Dumper::Sortkeys = 1;

        print Data::Dumper->Dump([$tc2],['tc2']);

    }

    return $algebra;
}

# convert an array of quoted strings/barewords into an array
# of normalized strings
sub _process_name_pieces
{
    my @pieces = @_;

    my @full_name;

    # turn array of name "pieces" back into full names
    for my $name_piece (@pieces)
    {
        # may need to distinguish between bareword and
        # quoted strings
        if (exists($name_piece->{quoted_string}))
        {
            my $p1 = $name_piece->{quoted_string};
            # strip leading/trailing quotes
            my @p2 = $p1 =~ m/^\"(.*)\"$/;
            push @full_name, @p2;
        }
        else
        {
            # XXX XXX: may need to uc or lc here...

            push @full_name, values(%{$name_piece});
        }
    }

    return @full_name;

}

# recursive function to decorate table info
#
# get table information from the dictionary
# number each table uniquely
#
sub _get_table_info # private
{
#    whoami;

    # NOTE: get the current subroutine name so it is easier 
    # to call recursively
    my $subname = (caller(0))[3];

    my $self = shift;
    # generic tree of hashes/arrays
    my ($genTree, $dict) = @_;

    my $treeCtx = $self->{tc1};

    # recursively convert all elements of array
    if (ref($genTree) eq 'ARRAY')
    {
        my $maxi = scalar(@{$genTree});
        $maxi--;
        for my $i (0..$maxi)
        {
            $genTree->[$i] = $self->$subname($genTree->[$i], $dict);
        }

    }
    if (ref($genTree) eq 'HASH')
    {
        keys( %{$genTree} ); # XXX XXX: need to reset expression!!
        # recursively convert all elements of hash, but treat
        # table name specially

        while ( my ($kk, $vv) = each ( %{$genTree})) # big while
        {
            if ($kk !~ m/^(table_name|table_alias)$/)
            {
                $genTree->{$kk} = $self->$subname($vv, $dict);
            }
            else # table name or alias
            {
                my $isTableName = ($kk =~ m/^table_name$/);
                if ($isTableName)
                {
                    # uniquely number each table reference
                    # Note: use for join order to select STAR expansion

                    $genTree->{tc_table_position} = $treeCtx->{tpos};
                    $treeCtx->{tpos}++;
                }

                my @full_name = _process_name_pieces(@{$vv});

                # build a "dot" separated string
                my $full_name_str = join('.', @full_name);

                if (!$isTableName)
                {
                    # don't build an alias unless we really have one
                    $genTree->{tc_table_fullalias} = $full_name_str
                        if (scalar(@{$vv}));
                }
                else # is table name
                {
                    $genTree->{tc_table_fullname} = $full_name_str;

                    # look it up in the dictionary
                    if (! ($dict->DictTableExists (
                                                   tname => $full_name_str,
                                                   silent_exists => 1,
                                                   silent_notexists => 0 
                                                   )
                           )
                        )
                    {
                        push @{$treeCtx->{tc_err}->{nosuch_table}}, 
                             $full_name_str;
#                       return undef; # XXX XXX XXX XXX
                    }
                    else
                    {
                        # XXX XXX: temporary?
                        # get hash by column name
                        $genTree->{tc_table_colhsh} = 
                            $dict->DictTableGetCols (tname => $full_name_str);
                        my @colarr;

                        (keys(%{$genTree->{tc_table_colhsh}}));
                        # build array by column position
                        while ( my ($chkk, $chvv) 
                                = each ( %{$genTree->{tc_table_colhsh}})) 
                        {
                            my %nh = (colname => $chkk, coltype => $chvv->[1]);
                            $colarr[$chvv->[0]] = \%nh;
                        }
                        shift @colarr;
                        $genTree->{tc_table_colarr} = \@colarr;               
                    }
                } # end is table name
            } # end table name or alias
        } # end big while
    }
    return $genTree;
}

# check the validity of results of _get_table_info
#
# determine proper table/alias name
# find duplicates
# associate table info with appropriate query block
# build list of query block dependency information for correlated subqueries
#
sub _check_table_info # private
{
#    whoami;

    # NOTE: get the current subroutine name so it is easier 
    # to call recursively
    my $subname = (caller(0))[3];

    my $self = shift;
    # generic tree of hashes/arrays
    my ($genTree, $dict) = @_;

    my $treeCtx = $self->{tc2};

    # recursively convert all elements of array
    if (ref($genTree) eq 'ARRAY')
    {
        my $maxi = scalar(@{$genTree});
        $maxi--;
        for my $i (0..$maxi)
        {
            $genTree->[$i] = $self->$subname($genTree->[$i], $dict);
        }
    }
    if (ref($genTree) eq 'HASH')
    {
        keys( %{$genTree} ); # XXX XXX: need to reset expression!!
        # recursively convert all elements of hash

        my $qb_setup = 0; # TRUE if top hash of query block

        if (exists($genTree->{query_block})) 
        {
            $qb_setup = 1;

            # keep track of current query block number
            my $current_qb = $genTree->{query_block};

            # push on the front
            unshift @{$treeCtx->{qb_list}}, $current_qb;

            unless (defined($treeCtx->{tablist}->[$current_qb]))
            {
                # build a hash to hold the table info associated with
                # the current query block
                $treeCtx->{tablist}->[$current_qb] = { 
                    tables => {}, 

                    # reserve space for select list column aliases
                    select_list_aliases => {},
                    select_col_num => 0
                };
            }

            if (exists($genTree->{query_block_parent}))
            {
                # save the query block dependency information
                my @foo = @{$genTree->{query_block_parent}};
                $treeCtx->{qb_dependency}->[$current_qb] = \@foo;
            }
        }

        while ( my ($kk, $vv) = each ( %{$genTree})) # big while
        {
            if ($kk !~ m/^tc_table_fullname$/)
            {
                $genTree->{$kk} = $self->$subname($vv, $dict);
            }
            else # table name 
            {
                my $tab_alias;
                
                if (exists($genTree->{tc_table_fullalias}))
                {
                    $tab_alias = $genTree->{tc_table_fullalias};
                }
                else
                {
                    $tab_alias = $vv;
                    # is this safe? update the hash we are traversing...
                    $genTree->{tc_table_fullalias} = $tab_alias;
                }

                # store table info in the table list for the current
                # query block
                my $current_qb = $treeCtx->{qb_list}->[0];
                my $tablist    = $treeCtx->{tablist}->[$current_qb]->{tables};

                # use the alias, rather than the tablename -- this is
                # ok since the alias points to the base table info.
                if (exists($tablist->{$tab_alias}))
                {
                    my $msg = "Found duplicate table name: " .
                        "\'$tab_alias\'\n";
                    my %earg = (self => $self, msg => $msg,
                                severity => 'warn');
                    
                    &$GZERR(%earg)
                        if (defined($GZERR));
                    # return undef # XXX XXX XXX
                }
                else
                {
                    # save a reference to current hash
                    $tablist->{$tab_alias} = $genTree;
                }

            } # end table name
        } # end big while

        if ($qb_setup)
        {
            # pop from the front
            shift @{$treeCtx->{qb_list}};
        }

    }
    return $genTree;
}

sub ColumnCheck
{
    my $self = shift;
    
    my %required = (
                    algebra   => "no algebra !",
                    statement => "no sql statement !",
                    dict      => "no dictionary !"
                    );
    
    my %args = ( # %optional,
                 @_);
    
    return undef
        unless (Validate(\%args, \%required));


    my $algebra = $args{algebra};

    my $tc3 = {}; # type check tree context for tree walker
    $self->{tc3} = $tc3;

    # local tree walk state
    $tc3->{qb_list} = []; # build an arr starting with current query block num
    $tc3->{statement} = $args{statement};

    # save bad columns for error reporting
    $tc3->{tc_err}->{duplicate_alias} = {};
    $tc3->{tc_err}->{nosuch_column}   = [];
    # use the table information from table typecheck phase
    $tc3->{tablist} = $self->{tc2}->{tablist};

    # convert "select * "  to "select <column_list> "
    $algebra = $self->_get_star_cols($algebra, $args{dict});

    # setup select list column aliases and column headers
    $algebra = $self->_get_col_alias($algebra, $args{dict});

    # map columns to FROM clause tables
    $algebra = $self->_get_col_info($algebra, $args{dict});

    # use type information to map sql comparison operations to their
    # perl equivalents

    $algebra = $self->_fixup_comp_op($algebra, $args{dict});

    $tc3->{AndPurity} = 1; # false if find OR's

    $algebra = $self->_sql_where($algebra, $args{dict});

    if (0) # XXX XXX XXX XXX
    {
        my $tc2 = $self->{tc2};

        local $Data::Dumper::Indent   = 1;
        local $Data::Dumper::Sortkeys = 1;

        print Data::Dumper->Dump([$tc2],['tc2']);

    }

    # NOTE: need to build the select list column aliases *first*,
    # then type check all columns.  
    #
    # Different standards (SQL92, SQL99) and different products have
    # different scoping and precedence rules on the select list column
    # aliases.  In general, the WHERE clause is processed before the
    # select list defines the column aliases, so it can only use table
    # and table alias information.  (Which makes sense -- you can have
    # a column alias on an aggregate operator like COUNT(*), which
    # can't be completely evaluated until the WHERE clause processes
    # the final row.)
    # ORDER BY is the last operation, so it can evaluate expressions
    # using the column aliases.  GROUP BY and HAVING behavior seems to
    # be a bit of a tossup.  We'll try to maintain some flexibility --
    # the tablist has separate entries column alias info and table
    # definitions in each query block.
    #
    # What is scope of column aliasing in select list itself?  left to
    # right (ie, col2 can utilize the col1 alias) or "simultaneous"?
    #
    #
    # Note that select list column aliases are allowed to mask table
    # columns, but all other table column references should not be
    # ambiguous.

    # XXX XXX XXX: _get_col_alias to only build up alias info in
    # tablist, then _get_col_info to resolve column names against
    # aliases, then tables if necessary

    return $algebra;
}

# expand STAR select lists...
#
#
sub _get_star_cols
{
#    whoami;

    # NOTE: get the current subroutine name so it is easier 
    # to call recursively
    my $subname = (caller(0))[3];

    my $self = shift;
    # generic tree of hashes/arrays
    my ($genTree, $dict) = @_;

    my $treeCtx = $self->{tc3};

    # recursively convert all elements of array
    if (ref($genTree) eq 'ARRAY')
    {
        my $maxi = scalar(@{$genTree});
        $maxi--;
        for my $i (0..$maxi)
        {
            $genTree->[$i] = $self->$subname($genTree->[$i], $dict);
        }

    }
    if (ref($genTree) eq 'HASH')
    {
        keys( %{$genTree} ); # XXX XXX: need to reset expression!!

        # convert subtree first, then process local select list
        {
            my $qb_setup = 0; # TRUE if top hash of query block
            
            if (exists($genTree->{query_block})) 
            {
                $qb_setup = 1;
                
                # keep track of current query block number
                my $current_qb = $genTree->{query_block};

                # push on the front
                unshift @{$treeCtx->{qb_list}}, $current_qb;
            }
            
            while ( my ($kk, $vv) = each ( %{$genTree})) # big while
            {
                # convert subtree first...
                $genTree->{$kk} = $self->$subname($vv, $dict);
            }

            if ($qb_setup)
            {
                # pop from the front
                shift @{$treeCtx->{qb_list}};
            }

        }

        # recursively convert all elements of hash

        my $qb_setup = 0; # TRUE if top hash of query block

        if (exists($genTree->{query_block})) 
        {
            $qb_setup = 1;

            # keep track of current query block number
            my $current_qb = $genTree->{query_block};

            # push on the front
            unshift @{$treeCtx->{qb_list}}, $current_qb;
        }

        if (exists($genTree->{select_list}))
        {
            # if the select list is STAR (not an array)
            unless (ref($genTree->{select_list}) eq 'ARRAY')
            {                
                # start in current query block
                # find our tablist
                my $current_qb   = $treeCtx->{qb_list}->[0];
                my $curr_tablist = $treeCtx->{tablist}->[$current_qb];

                my $table_cnt = keys( %{$curr_tablist->{tables}} ); # reset

                my @tab_cols;

                while ( my ($hkk, $hvv) = 
                        each (%{$curr_tablist->{tables}}))
                {
                    my $tpos = $hvv->{tc_table_position};

                    my $col_list = [];
                    
                    # get all the column names
                    for my $colh (@{$hvv->{tc_table_colarr}})
                    {
                        push @{$col_list}, $colh->{colname};
                    }

                    # convert to array of value expressions 
                    for my $colcnt (0..(scalar(@{$col_list})-1))
                    {
                        # quote the strings to preserve case
                        my $cv = 
                        {quoted_string => '"' . $col_list->[$colcnt] . '"'};

                        # table name doesn't change, but building a
                        # new one each time gives a nicer Data::Dumper
                        # output...
                        my $table_name = 
                        {quoted_string => '"' . $hkk .'"' };

                        my $foo = [];

                        if ($table_cnt > 1)
                        {
                            # don't use table name if only one table
                            push @{$foo}, $table_name;
                        }
                        push @{$foo}, $cv;

                        # build the value expression
                        $col_list->[$colcnt] 
                            = {
                                col_alias => [],
                                value_expression => {
                                    column_name => $foo
                                }
                            };
                    }
                    # store tables in tpos order
                    $tab_cols[$tpos] = $col_list;


                } # end each tablist table

                
                my $sel_list = [];
                for my $tabi (@tab_cols)
                {
                    if (defined($tabi) && scalar(@{$tabi}))
                    {
                        push @{$sel_list}, @{$tabi};
                    }
                }

                $genTree->{select_list} = $sel_list;
            }
        }

        if ($qb_setup)
        {
            # pop from the front
            shift @{$treeCtx->{qb_list}};
        }

    }
    return $genTree;
}

# get column aliases and column "headers"
#
#
sub _get_col_alias # private
{
#    whoami;

    # NOTE: get the current subroutine name so it is easier 
    # to call recursively
    my $subname = (caller(0))[3];

    my $self = shift;
    # generic tree of hashes/arrays
    my ($genTree, $dict) = @_;

    my $treeCtx = $self->{tc3};

    # recursively convert all elements of array
    if (ref($genTree) eq 'ARRAY')
    {
        my $maxi = scalar(@{$genTree});
        $maxi--;
        for my $i (0..$maxi)
        {
            $genTree->[$i] = $self->$subname($genTree->[$i], $dict);
        }

    }
    if (ref($genTree) eq 'HASH')
    {
        keys( %{$genTree} ); # XXX XXX: need to reset expression!!
        # recursively convert all elements of hash

        my $qb_setup = 0; # TRUE if top hash of query block

        if (exists($genTree->{query_block})) 
        {
            $qb_setup = 1;

            # keep track of current query block number
            my $current_qb = $genTree->{query_block};

            # push on the front
            unshift @{$treeCtx->{qb_list}}, $current_qb;
        }

        while ( my ($kk, $vv) = each ( %{$genTree})) # big while
        {
            if ($kk !~ m/^(column_name|col_alias)$/)
            {
                $genTree->{$kk} = $self->$subname($vv, $dict);
            }
            else # column name or alias
            {
                my $isColumnName = ($kk =~ m/^column_name$/);

                my @full_name = _process_name_pieces(@{$vv});

                # last portion should be column name (if not an alias)
                my $column_name;
                $column_name = pop @full_name
                    if ($isColumnName);

                # build a "dot" separated string
                my $full_name_str = join('.', @full_name);

                if ($isColumnName)
                {
                    # just build the names here -- lookup in dictionary later
                    $genTree->{tc_col_tablename} = $full_name_str
                        if (scalar(@full_name));
                    $genTree->{tc_column_name}   = $column_name;
                }
                else # column alias
                { 
                    # don't build an alias unless we really have one
                    if (scalar(@full_name))
                    {
                        # alias for later reference
                        $genTree->{tc_col_fullalias} = $full_name_str;
                        
                        # column "header" for formatting output is the
                        # same as the alias
                        $genTree->{tc_col_header}    = $full_name_str;
                        
                        # start in current query block
                        # find our tablist
                        # add our new select list column alias
                        my $current_qb   = $treeCtx->{qb_list}->[0];
                        my $curr_tablist = $treeCtx->{tablist}->[$current_qb];

                        my $qb_aliases     = 
                            $curr_tablist->{select_list_aliases};
                        my $select_col_num = 
                            $curr_tablist->{select_col_num};
                        $curr_tablist->{select_col_num} += 1;
                        
                        if (exists($qb_aliases->{$full_name_str}))
                        {
                            # error: duplicate alias
                            my $dupa = 
                                $treeCtx->{tc_err}->{duplicate_alias};

                            if (exists($dupa->{$full_name_str}))
                            {
                                # count duplicates!
                                $dupa->{$full_name_str} += 1;
                            }
                            else
                            {
                                $dupa->{$full_name_str} = 1;
                            }

                            # XXX XXX: is this illegal?

                            my $msg = "duplicate alias: " .
                                "\'$full_name_str\'";

                            my %earg = (self => $self, msg => $msg,
                                        severity => 'warn');
                            
                            &$GZERR(%earg)
                                if (defined($GZERR));
                            
                            # XXX XXX XXX return undef
                        }
                        else # update the alias with position info
                        {
                            # XXX XXX XXX: what else goes here?

                            my $foo = {};
                            $foo->{p1} = $genTree->{p1};
                            $foo->{p2} = $genTree->{p2};
                            $foo->{select_col_num} = $select_col_num;
                            $qb_aliases->{$full_name_str} = $foo;

                        }
                        
                    }
                    else # no alias
                    {
                        # derive column "header" from input txt -- the
                        # default header is just the text of the
                        # expression.  
                        my $col_hd;

                        if (exists($genTree->{p1}))
                        {
                            $col_hd = 
                                substr($treeCtx->{statement},
                                       $genTree->{p1},
                                       ($genTree->{p2} - $genTree->{p1}) + 1
                                       );
                            $col_hd =~ s/^\s*//; # trim leading spaces
                        }
                        else
                        {
                            # XXX XXX: generated col for STAR - fake it
                            
                            # XXX XXX: assume have a column name
                            my $npa = 
                                $genTree->{value_expression}->{column_name};
                            
                            my @col_name = 
                                _process_name_pieces(@{$npa});

                            $col_hd = join(".", @col_name);
                        }
                        
                        $genTree->{tc_col_header}    = $col_hd;
                    }
                }  # end col alias
            }
        } # end big while

        if ($qb_setup)
        {
            # pop from the front
            shift @{$treeCtx->{qb_list}};
        }

    }
    return $genTree;
}

# recursive function to decorate column info
#
#
sub _get_col_info # private
{
#    whoami;

    # NOTE: get the current subroutine name so it is easier 
    # to call recursively
    my $subname = (caller(0))[3];

    my $self = shift;
    # generic tree of hashes/arrays
    my ($genTree, $dict) = @_;

    my $treeCtx = $self->{tc3};

    # recursively convert all elements of array
    if (ref($genTree) eq 'ARRAY')
    {
        my $maxi = scalar(@{$genTree});
        $maxi--;
        for my $i (0..$maxi)
        {
            $genTree->[$i] = $self->$subname($genTree->[$i], $dict);
        }

    }
    if (ref($genTree) eq 'HASH')
    {
        keys( %{$genTree} ); # XXX XXX: need to reset expression!!
        # recursively convert all elements of hash

        my $qb_setup = 0; # TRUE if top hash of query block

        if (exists($genTree->{query_block})) 
        {
            $qb_setup = 1;

            # keep track of current query block number
            my $current_qb = $genTree->{query_block};

            # push on the front
            unshift @{$treeCtx->{qb_list}}, $current_qb;
        }

      L_bigw:
        while ( my ($kk, $vv) = each ( %{$genTree})) # big while
        {
            if ($kk !~ m/^(tc_column_name)$/)
            {
                $genTree->{$kk} = $self->$subname($vv, $dict);
            }
            else # column name 
            {
                my $full_name_str = undef;
                if (exists($genTree->{tc_col_tablename}))
                {
                    $full_name_str = $genTree->{tc_col_tablename};
                }
                my $column_name = $genTree->{tc_column_name};

                # XXX XXX XXX: need to deal with table.rid...
                if ($column_name =~ m/^(rid|rownum)$/i)
                {
                    if ($column_name =~ m/^(rid)$/i)
                    {
                        $genTree->{tc_expr_type} = 'c';
                    }
                    else
                    {
                        $genTree->{tc_expr_type} = 'n';
                    }

                    # XXX XXX: need to deal with other pseudo cols like 
                    # sysdate...

                    # rid and rownum are valid
                    next L_bigw;
                }

                my $foundCol = 0;

                # start in current query block
                my $current_qb = $treeCtx->{qb_list}->[0];
                
                # NOTE: search backward from most recent
                # (innermost) query block to earliest (outermost)
              L_qb:
                for (my $qb_num = $current_qb;
                     $qb_num > 0;
                     $qb_num--)
                {
                    my $qb2 = $treeCtx->{tablist}->[$qb_num]->{tables};
                    
                    # if have a tablename, look there
                    if (defined($full_name_str))
                    {
                        next L_qb
                            unless (exists($qb2->{$full_name_str}));
                        
                        my $h1 = $qb2->{$full_name_str}->{tc_table_colhsh};
                        next L_qb
                            unless (exists($h1->{$column_name}));

                        $genTree->{tc_column_num} = 
                            $h1->{$column_name}->[0];
                        $genTree->{tc_expr_type} = 
                            $h1->{$column_name}->[1];
                        $genTree->{tc_column_qb} = $qb_num;
                        $foundCol = 1;
                        last L_qb; # done!
                    }
                    else
                    {
                        # need to check all tables in block
                        
                        keys( %{$qb2} ); # XXX XXX: need to reset 

                      L_littlew:
                        while ( my ($hkk, $hvv) = 
                                each ( %{$qb2})) # little while
                        {
                            my $h1 = $hvv->{tc_table_colhsh};
                            next L_littlew
                                unless (exists($h1->{$column_name}));

                            # check all tables in current query block
                            # for duplicate column names
                            if ($foundCol)
                            {
                                my $msg = "column name " .
                                    "\'$column_name\' is ambiguous -- ";

                                $msg .= "tables \'" .
                                    $genTree->{tc_col_tablename} . 
                                    "\', \'" . $hkk . "\'";

                                my %earg = (self => $self, msg => $msg,
                                            severity => 'warn');
                                
                                &$GZERR(%earg)
                                    if (defined($GZERR));
                                
                                last L_qb;
                            }
                                

                            # set the table name
                            $genTree->{tc_col_tablename} = $hkk;
                            
                            $genTree->{tc_column_num} = 
                                $h1->{$column_name}->[0];
                            $genTree->{tc_expr_type} = 
                                $h1->{$column_name}->[1];
                            $genTree->{tc_column_qb} = $qb_num;
                            $foundCol = 1;
#                                last L_qb;
                        } # end little while

                        last L_qb
                            if ($foundCol);
                    }
                } # end for each qb num
                unless ($foundCol)
                {
                    push @{$treeCtx->{tc_err}->{nosuch_column}}, 
                         $full_name_str;
                    
                    my $msg = "column \'$column_name\' not found\n";

                    my %earg = (self => $self, msg => $msg,
                                severity => 'warn');
                    
                    &$GZERR(%earg)
                        if (defined($GZERR));
                    
#                       return undef; # XXX XXX XXX XXX
                }

            } # end is col name
        } # end big while

        if ($qb_setup)
        {
            # pop from the front
            shift @{$treeCtx->{qb_list}};
        }
        
    }
    return $genTree;
}

    # transform standard sql relational operators to Perl-style,
    # distinguishing numeric and character comparisons
    my $relop_map = 
    {
        '==' => { "n" => "==",  "c" => "eq"},
        '='  => { "n" => "==",  "c" => "eq"},
        '<>' => { "n" => "!=",  "c" => "ne"},
        '!=' => { "n" => "!=",  "c" => "ne"},
        '>'  => { "n" => ">",   "c" => "gt"},
        '<'  => { "n" => "<",   "c" => "lt"},
        '>=' => { "n" => ">=",  "c" => "ge"},
        '<=' => { "n" => "<=",  "c" => "le"},

        '<=>' => { "n" => "<=>",  "c" => "cmp"}
    };


# comp_op fixup
#
#
sub _fixup_comp_op
{
#    whoami;

    # NOTE: get the current subroutine name so it is easier 
    # to call recursively
    my $subname = (caller(0))[3];

    my $self = shift;
    # generic tree of hashes/arrays
    my ($genTree, $dict) = @_;

    my $treeCtx = $self->{tc3};

    # recursively convert all elements of array
    if (ref($genTree) eq 'ARRAY')
    {
        my $maxi = scalar(@{$genTree});
        $maxi--;
        for my $i (0..$maxi)
        {
            $genTree->[$i] = $self->$subname($genTree->[$i], $dict);
        }

    }
    if (ref($genTree) eq 'HASH')
    {
        keys( %{$genTree} ); # XXX XXX: need to reset expression!!

        # convert subtree first, then process local select list
        {
            my $qb_setup = 0; # TRUE if top hash of query block
            
            if (exists($genTree->{query_block})) 
            {
                $qb_setup = 1;
                
                # keep track of current query block number
                my $current_qb = $genTree->{query_block};

                # push on the front
                unshift @{$treeCtx->{qb_list}}, $current_qb;
            }
            
            while ( my ($kk, $vv) = each ( %{$genTree})) # big while
            {
                # convert subtree first...
                $genTree->{$kk} = $self->$subname($vv, $dict);
            }

            if ($qb_setup)
            {
                # pop from the front
                shift @{$treeCtx->{qb_list}};
            }

        }

        # recursively convert all elements of hash

        my $qb_setup = 0; # TRUE if top hash of query block

        if (exists($genTree->{query_block})) 
        {
            $qb_setup = 1;

            # keep track of current query block number
            my $current_qb = $genTree->{query_block};

            # push on the front
            unshift @{$treeCtx->{qb_list}}, $current_qb;
        }

        # grab the WHERE clause text
        if (exists($genTree->{sc_tree}))
        {
            if (exists($genTree->{p1})
                && exists($genTree->{p2}))
            {
                my $pos1 = $genTree->{p1};
                my $pos2 = $genTree->{p2};

                my $sc_txt =
                    substr($treeCtx->{statement},
                           $pos1,
                           ($pos2 - $pos1) + 1
                           );
                
                $genTree->{sc_txt} = $sc_txt;
            }
        }

        if (exists($genTree->{comp_op}))
        {
#            print $genTree->{operator}, "\n";

            # fixup the perl operators
            if (($genTree->{comp_op} eq 'comp_perlish')
                && (3 == scalar(@{$genTree->{operands}})))
            {
                my $op1 = 
                    $genTree->{operands}->[1];
                $genTree->{operands}->[1] = {
                    tc_comp_op   => $op1,
                    orig_comp_op => $op1
                    };

                my $op2 =
                    $genTree->{operands}->[2];

                # XXX XXX: op2 should be an array of 
                # perl regex pieces -- reassemble it.  
                # may need to do some work for non-standard
                # quoting
                my $perl_lit = join("", @{$op2});

                $genTree->{operands}->[2] = {
                    string_literal => $perl_lit,
                    orig_reg_exp => $op2
                    }
            }

          L_for_ops:
            for my $op_idx (0..(@{$genTree->{operands}}-1))
            {
                my $op1 = $genTree->{operands}->[$op_idx]; 

#                print $op1, "\n", ref($op1), "\n";

                next L_for_ops
                    if (ref($op1)); # ref is false for scalar non-ref

#                print $op1, "\n";

                my $tok_expr = '(<=>|cmp|eq|==|<>|lt|gt|le|ge|!=|<=|>=|<|>|=)';

                next L_for_ops
                    unless ($op1 =~ m/^$tok_expr$/);

                next L_for_ops
                    unless (exists($relop_map->{$op1}));

                my $h1 = $relop_map->{$op1};

                my $left_op  = $genTree->{operands}->[$op_idx - 1]; 
                my $right_op = $genTree->{operands}->[$op_idx + 1]; 

                my $op_type = '?';

                if ((ref($left_op) eq 'HASH') && 
                    (exists($left_op->{tc_expr_type})))
                {
                    $op_type = $left_op->{tc_expr_type};
                } # else type is char by default

                # char takes precedence over number, so only test
                # right side if left side was numeric
                if (($op_type ne 'c') &&
                    (ref($right_op) eq 'HASH') && 
                    (exists($right_op->{tc_expr_type})))
                {
                    $op_type = $right_op->{tc_expr_type};
                }
                
                $op_type = 'c' # only allow c or n
                    unless ($op_type =~ m/^(n|c)$/);

                # update the operator 
                $genTree->{operands}->[$op_idx] = {
                    tc_comp_op   => $h1->{$op_type},
                    orig_comp_op => $op1
                    };

            } # end for
        }

        if ($qb_setup)
        {
            # pop from the front
            shift @{$treeCtx->{qb_list}};
        }

    }
    return $genTree;
}

# sqlwhere
#
sub _sql_where
{
#    whoami;

    # NOTE: get the current subroutine name so it is easier 
    # to call recursively
    my $subname = (caller(0))[3];

    my $self = shift;
    # generic tree of hashes/arrays
    my ($genTree, $dict) = @_;

    my $treeCtx = $self->{tc3};

    # recursively convert all elements of array
    if (ref($genTree) eq 'ARRAY')
    {
        my $maxi = scalar(@{$genTree});
        $maxi--;
        for my $i (0..$maxi)
        {
            $genTree->[$i] = $self->$subname($genTree->[$i], $dict);
        }

    }
    if (ref($genTree) eq 'HASH')
    {
        keys( %{$genTree} ); # XXX XXX: need to reset expression!!

        # convert subtree first, then process local select list
        {
            my $qb_setup = 0; # TRUE if top hash of query block
            
            if (exists($genTree->{query_block})) 
            {
                $qb_setup = 1;
                
                # keep track of current query block number
                my $current_qb = $genTree->{query_block};

                # push on the front
                unshift @{$treeCtx->{qb_list}}, $current_qb;
            }
            
            while ( my ($kk, $vv) = each ( %{$genTree})) # big while
            {
                # convert subtree first...
                $genTree->{$kk} = $self->$subname($vv, $dict);
            }

            if ($qb_setup)
            {
                # pop from the front
                shift @{$treeCtx->{qb_list}};
            }

        }

        # recursively convert all elements of hash

        my $qb_setup = 0; # TRUE if top hash of query block

        if (exists($genTree->{query_block})) 
        {
            $qb_setup = 1;

            # keep track of current query block number
            my $current_qb = $genTree->{query_block};

            # push on the front
            unshift @{$treeCtx->{qb_list}}, $current_qb;
        }

        if (exists($genTree->{tc_column_name}))
        {
            if (exists($genTree->{tc_column_num}))
            {
                $genTree->{vx} = 
                    '$outarr->[' . ($genTree->{tc_column_num} - 1) .
                    ']';
            }
            else
            {
                if ($genTree->{tc_column_name} =~ m/^rid$/i)
                {
#                    $genTree->{vx} = '$tc_rid';
                    $genTree->{vx} = '$rid';
                }
                if ($genTree->{tc_column_name} =~ m/^rownum$/i)
                {
                    $genTree->{vx} = '$tc_rownum';
                }
            }
        }

        if (exists($genTree->{numeric_literal}))
        {
            $genTree->{vx} = $genTree->{numeric_literal};
        }

        if (exists($genTree->{string_literal}))
        {
            $genTree->{vx} = $genTree->{string_literal};
        }

        if (exists($genTree->{comp_op}))
        {
            my $bigstr = '( ';
            for my $op1 (@{$genTree->{operands}})
            {
                if (ref($op1) eq 'HASH')
                {
                    if (exists($op1->{vx}))
                    {
                        $bigstr .= $op1->{vx} . ' ';
                    }
                    if (exists($op1->{tc_comp_op}))
                    {
                        $bigstr .= $op1->{tc_comp_op} . ' ';
                    }
                }
            }
            $bigstr .= ')';
            $genTree->{vx} = $bigstr;

            # build an index key if have an expression like:
            # column_name comp_op literal
            #
            if (3 == scalar(@{$genTree->{operands}}))
            {
                my $oplist = $genTree->{operands};
                my ($colnum, $comp_op, $literal);
                
                if (ref($oplist->[0]) eq 'HASH')
                {
                    $colnum = ($oplist->[0]->{tc_column_num}) - 1
                        if (exists($oplist->[0]->{tc_column_num}));
                }
                if (ref($oplist->[1]) eq 'HASH')
                {
                    my $tok_expr = '(eq|==|<|>|lt|gt|le|ge|<=|>=)';

                    $comp_op = $oplist->[1]->{tc_comp_op}
                        if (exists($oplist->[1]->{tc_comp_op})
                            && ($oplist->[1]->{tc_comp_op} =~
                                m/^$tok_expr$/
                                )
                            );
                }
                if (ref($oplist->[2]) eq 'HASH')
                {
                    if (exists($oplist->[2]->{string_literal}))
                    {
                        $literal = $oplist->[2]->{string_literal};
                    }
                    elsif (exists($oplist->[2]->{numeric_literal}))
                    {
                        $literal = $oplist->[2]->{numeric_literal};
                    }
                }

                if (defined($colnum)  && 
                    defined($comp_op) && 
                    defined($literal))
                {
                    # XXX XXX: change to better format
                    $genTree->{tc_index_key} =
                        [
                         { col     => $colnum  },
                         { op      => $comp_op },
                         { literal => $literal }
                         ];
                }
                
            } # end build index key
        }

        if (exists($genTree->{IS}))
        {
            my $bigstr = '';

            # XXX XXX XXX: not an array!
            my $op1 = $genTree->{operands};
            {
                if (exists($op1->{vx}))
                {
                    $bigstr .= $op1->{vx} . ' ';
                }
            }

            my $tfn1 = $genTree->{IS}->[0]->{TFN}->[0];
            my $not1 = scalar(@{$genTree->{IS}->[0]->{not}});

            my $s2;
            if ($tfn1 =~ m/null/i)
            {
                # not null = is defined
                $s2 = '(';
                $s2 .= '!' if (!$not1);
                $s2 .= 'defined(' . $bigstr . '))';

                # NOTE: Reset AndPurity if have "IS NULL" predicate
                # because can't do index search on null values.
                $treeCtx->{AndPurity} = 0
                    unless ($not1);

            }
            else
            {
                # not true = is false
                $s2 = '(('.$bigstr .') == ';
                my $tf_val = ($not1) ? 1 : 0;
                if ($tfn1 =~ m/true/i)
                {
                    $tf_val = ($not1) ? 0 : 1;
                }
                $s2 .= $tf_val . ')';
            }
            $genTree->{vx} = $s2;
            
        }

        if (exists($genTree->{math_op}))
        {
            my $bigstr = '( ';
            for my $op1 (@{$genTree->{operands}})
            {
                if (ref($op1))
                {
                    if (exists($op1->{vx}))
                    {
                        $bigstr .= $op1->{vx} . ' ';
                    }
                }
                else
                {
                    # XXX XXX: concatenation
                    if ($op1 eq '||')
                    { $op1 = '.'; }

                    $bigstr .= $op1 . ' ';
                }
            }
            $bigstr .= ')';
            $genTree->{vx} = $bigstr;
        }

        if (exists($genTree->{bool_op}))
        {
            my $bigstr = '( ';

            my $op_cnt = 0;
            for my $op1 (@{$genTree->{operands}})
            {
                if (ref($op1) eq 'HASH')
                {
                    if (exists($op1->{vx}))
                    {
                        $bigstr .= $op1->{vx} . ' ';
                    }
                }
                elsif (ref($op1) eq 'ARRAY')
                {
                    if ($op1->[0] =~ m/^or$/i)
                    {
                        # found an OR
                        $treeCtx->{AndPurity} = 0;

                        $bigstr .= '|| ';
                    }
                    elsif ($op1->[0] =~ m/^and$/i)
                    {
                        $bigstr .= '&& ';
                    }
                    else
                    {
                        $bigstr .= $op1->[0] . ' ';
                    }

                }
                else
                {
                    $bigstr .= $op1 . ' ';
                }

                $op_cnt++;
            }
            $bigstr .= ')';
            $genTree->{vx} = $bigstr;
        }

        if (exists($genTree->{function_name}))
        {
            my $bigstr = ' ' . $genTree->{function_name} . '( ';
            
            # 

            if (exists($genTree->{operands})
                && (ref($genTree->{operands}) eq 'ARRAY')
                && scalar(@{$genTree->{operands}}))
            {
                # XXX XXX: deal with ALL/DISTINCT/SUBQUERIES

                my $fn_ops = $genTree->{operands}->[0];

                # XXX XXX: what about COUNT(*), ECOUNT?
                if (exists($fn_ops->{operands})
                    && (ref($fn_ops->{operands}) eq 'ARRAY'))
                {
                    my $cnt_ff = 0;
                    for my $op1 (@{$fn_ops->{operands}})
                    {
                        if (exists($op1->{vx}))
                        {
                            $bigstr .= ',' if ($cnt_ff);
                            $bigstr .= ' ' . $op1->{vx} ;
                        }
                        $cnt_ff++;
                    }
                }
            }
            $bigstr .= ')';
            $genTree->{vx} = $bigstr;
        } # end function name


        if ($qb_setup)
        {
            # pop from the front
            shift @{$treeCtx->{qb_list}};
        }

    }
    return $genTree;
}


sub GetFromWhereEtc
{
    my $self = shift;
    
    my %required = (
                    algebra   => "no algebra !",
                    dict      => "no dictionary !"
                    );
    
    my %args = ( # %optional,
                 @_);
    
    return undef
        unless (Validate(\%args, \%required));


    my $algebra = $args{algebra};

    my $tc4 = {}; # type check tree context for tree walker
    $self->{tc4} = $tc4;

    # local tree walk state
    $tc4->{qb_list} = []; # build an arr starting with current query block num
    $tc4->{index_keys} = []             # only build index keys 
        if ($self->{tc3}->{AndPurity}); # if pure AND search condition

    $algebra = $self->_get_from_where($algebra, $args{dict});

    my $from       = $tc4->{from};
    my $sel_list   = $tc4->{select_list};
    my $where      = $tc4->{where};

    # XXX XXX XXX: need to localize AndPurity per WHERE clause/search cond
    my $and_purity = $self->{tc3}->{AndPurity};

    $tc4->{where}->[0]->{sc_and_purity} = $and_purity;
    if ($and_purity)
    {
        $tc4->{where}->[0]->{sc_index_keys} = $tc4->{index_keys};
    }
    # XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX 

    return ($algebra, $from, $sel_list, $where);
}

# transition from old parser to new...
#
sub _get_from_where
{
#    whoami;

    # NOTE: get the current subroutine name so it is easier 
    # to call recursively
    my $subname = (caller(0))[3];

    my $self = shift;
    # generic tree of hashes/arrays
    my ($genTree, $dict) = @_;

    my $treeCtx = $self->{tc4};

    # recursively convert all elements of array
    if (ref($genTree) eq 'ARRAY')
    {
        my $maxi = scalar(@{$genTree});
        $maxi--;
        for my $i (0..$maxi)
        {
            $genTree->[$i] = $self->$subname($genTree->[$i], $dict);
        }

    }

    if (ref($genTree) eq 'HASH')
    {
        keys( %{$genTree} ); # XXX XXX: need to reset expression!!

        # recursively convert all elements of hash

        my $qb_setup = 0; # TRUE if top hash of query block

        if (exists($genTree->{query_block})) 
        {
            $qb_setup = 1;

            # keep track of current query block number
            my $current_qb = $genTree->{query_block};

            # push on the front
            unshift @{$treeCtx->{qb_list}}, $current_qb;
        }

        if (scalar(@{$treeCtx->{qb_list}}))
        {
            my $current_qb = $treeCtx->{qb_list}->[0];
            
            if ($current_qb == 1)
            {

                if (exists($genTree->{from_clause}))
                {
                    $treeCtx->{from} = $genTree->{from_clause};
                }
                if (exists($genTree->{select_list}))
                {
                    $treeCtx->{select_list} = $genTree->{select_list};
                }
                if (exists($genTree->{search_cond}))
                {
                    $treeCtx->{where} = $genTree->{search_cond};
                }
            }
        }

        while ( my ($kk, $vv) = each ( %{$genTree})) # big while
        {
            if (($kk =~ m/tc_index_key/) &&
                exists($treeCtx->{index_keys}))
            {
                # build big list of index keys
                push @{$treeCtx->{index_keys}}, @{$vv};
            }
            # convert subtree first...
            $genTree->{$kk} = $self->$subname($vv, $dict);
        }

        if ($qb_setup)
        {
            # pop from the front
            shift @{$treeCtx->{qb_list}};
        }

    }
    return $genTree;
}



sub convert_valex
{
    my ($expr, $VX, $txt) = @_;
    my @narg;
    push @narg, $expr, $VX, $txt;

    # recursively convert all elements of array
#    print "ll: ",ref($expr),"\n";

    if (!ref($expr))
    {
        # the VX of a scalar is itself...
        $narg[1] = $expr;
    }
    if (ref($expr) eq 'ARRAY')
    {
#        print "a1\n";
        my $maxi = scalar(@{$expr});
        $maxi--;
        for my $i (0..$maxi)
        {
            $narg[0] = $expr->[$i];
            my @foo = convert_valex(@narg);
            $expr->[$i] = $foo[0];
        }
        $narg[0] = $expr;
    }
    if (ref($expr) eq 'HASH')
    {
#        print "h1\n";

        if (exists($expr->{search_cond}))
        {
            for my $search_item (@{$expr->{search_cond}})
            {
#                for my $search_item (@{$ss1})
#                {
                    print "s1: ",Data::Dumper->Dump([$search_item],['search_item']), "\n";
                    $narg[0] = $search_item;
                    my @foo = convert_valex(@narg);
                    $search_item->{CX} = $foo[1];

#                    if (exists($search_item->{'comparison_predicate'}))
#                    {
#                        $narg[0] = $search_item->{'comparison_predicate'};
#                        my @foo = convert_valex(@narg);
#                        $search_item->{CX} = $foo[1];
#                    }
#                }
            }
        }

        if (exists($expr->{select_list}))
        {
#            print "foo";

            for my $sel_item (@{$expr->{select_list}})
            {
#            print "bar";
#                greet $sel_item;

                if (exists($sel_item->{value_expression}))
                {
#                    my %vxcopy;
#                    eval (Data::Dumper->Dump([$sel_item->{value_expression}],
#                                            [qw(*vxcopy)]));

                    $narg[0] = $sel_item->{value_expression};
                    my @foo = convert_valex(@narg);
                    $sel_item->{VX} = $foo[1];
                    $sel_item->{col_text} = substr($narg[2], 
                                                   $sel_item->{p1},
                                                   ($sel_item->{p2} -
                                                   $sel_item->{p1}) + 1
                                                   );
                }
            }
        }

        if (exists($expr->{numeric_literal}))
        {
            @narg = ();
            push @narg, $expr,
            $expr->{numeric_literal},
            $txt;

            return @narg;
        }
        elsif (exists($expr->{string_literal}))
        {
            @narg = ();
            push @narg, $expr,
            $expr->{string_literal},
            $txt;

            return @narg;
        }
        elsif (exists($expr->{column_name}))
        {
            my $colname;
            for my $namedef (@{$expr->{column_name}})
            {
                my $namepiece;
                if (exists($namedef->{bareword}))
                {
                    $namepiece = $namedef->{bareword};
                }
                else
                {
                    $namepiece = $namedef->{quoted_string};
                }
                if (defined($colname))
                { $colname .= '.' }
                else {$colname = ""}
                $colname .= $namepiece;
            }
            @narg = ();
            push @narg, $expr,
            $colname,
            $txt;

            return @narg;
        }
        elsif (exists($expr->{math_op}))
        {
            if  (exists($expr->{operands}))
            {

                my @foo;
                for my $op (@{$expr->{operands}})
                {
                    $narg[0] = $op;

                    my @outi = convert_valex(@narg);

#                    print Data::Dumper->Dump(\@outi), "\n";

                    push @foo, $outi[1];
                    
                    # very simple constant folding for numeric literals
                    if (scalar(@foo) > 2)
                    {
                        if ($foo[-2] =~ m/\|\|/)
                        {
                            # concatenation
                            my $e2 = "concatenate( " . 
                                $foo[-3] . ", " . $foo[-1] . " )";
                            splice @foo, -3;
                            push @foo, $e2;
                        }
                        elsif (($foo[-1] =~ m/^\s*\d+\s*$/)
                               &&($foo[-3] =~ m/^\s*\d+\s*$/)
                               && ($foo[-2] =~ m/\+|\-|\*|\//))
                        {
                            my $e1 = "$foo[-3] $foo[-2] $foo[-1]";
                            print $e1, " = ";
                            my $e2 = eval($e1);
                            print $e2, "\n";
                            splice @foo, -3;
                            push @foo, $e2;
                        }
                    }
                    

                }
                if (scalar(@foo) > 1)
                {
                    $narg[1] = '( ' . join(" ", @foo) . ' )';
                }
                else
                {
                    $narg[1] = $foo[0];
                }
            }
            $narg[0] = $expr;
        }
        elsif (exists($expr->{function_name}))
        {
            # double-nested operands list in functions
            # XXX XXX: don't forget ALL/DISTINCT
            if  (exists($expr->{operands}))
            {
                my @foo;
                for my $op (@{$expr->{operands}})
                {
                    if  (exists($op->{operands}))
                    {
                        for my $op2 (@{$op->{operands}})
                        {
                            $narg[0] = $op2;

                            my @outi = convert_valex(@narg);
                            push @foo, $outi[1];

                        }
                    }
                    else
                    {
                        # maybe a subquery
                        $narg[0] = $op;
                        my @outi = convert_valex(@narg);
                    }
                }
#                print Data::Dumper->Dump(\@foo,['foo']);
                $narg[1] = $expr->{function_name};
                if (scalar(@foo) > 1)
                {
                    $narg[1] .= '( ' . join(",", @foo) . ' )';
                }
                else
                {
                    $narg[1] .= '(';
                    $narg[1] .= $foo[0]
                        if (scalar(@foo));
                    $narg[1] .= ')';
                }
            }
            $narg[0] = $expr;
        }
        elsif (exists($expr->{comp_op}))
        {
            if  (exists($expr->{operands}))
            {

                my @foo;
                for my $op (@{$expr->{operands}})
                {
                    $narg[0] = $op;

                    my @outi = convert_valex(@narg);

#                    print Data::Dumper->Dump(\@outi), "\n";

                    push @foo, $outi[1];

                }
                if (scalar(@foo) > 1)
                {
                    $narg[1] = '( ' . join(" ", @foo) . ' )';
                }
                else
                {
                    $narg[1] = $foo[0];
                }
            }
            $narg[0] = $expr;
        }
        elsif (exists($expr->{bool_op}))
        {
            if  (exists($expr->{operands}))
            {

                my $twiddle = -1;

                my @foo;
                for my $op (@{$expr->{operands}})
                {
                    $twiddle *= -1; # count every other op

                    if ((ref($op) eq 'ARRAY')
                        && ($twiddle < 0))
                    {
                        # deref the array for the operator name
                        $op = $op->[0];
                    }

                    $narg[0] = $op;

                    my @outi = convert_valex(@narg);

#                    print Data::Dumper->Dump(\@outi), "\n";

                    push @foo, $outi[1];

                }
                if (scalar(@foo) > 1)
                {
                    $narg[1] = '( ' . join(" ", @foo) . ' )';
                }
                else
                {
                    $narg[1] = $foo[0];
                }
            }
            $narg[0] = $expr;
        }
        else
        {
#            print "h2\n";
#            print Data::Dumper->Dump([$expr],['expr']);
            keys( %{$expr} ); # XXX XXX: need to reset expression!!
            
            while ( my ($kk2, $vv2) = each ( %{$expr} ) ) # big while
            {
                print "k2: $kk2\n";

                $narg[0] = $vv2;

                my @outi = convert_valex(@narg);

#                print "k: ", $kk2, "\n";
                $expr->{$kk2} = $narg[0];
            }
            $narg[0] = $expr;
        }
    }       
    return @narg;
} # end convert_valex


END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Plan::TypeCheck - Perform checks on relational algebra representation

=head1 SYNOPSIS

use Genezzo::Plan::TypeCheck;


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

=item need to handle FROM clause subqueries -- some tricky column type issues.

=item explode STARs with column names - need consistent join table position

=item check bool_op - AND purity if no OR's.

=item check relational operator (comp_op, relop)

=item handle ddl/dml (create, insert, delete etc with embedded queries) by
      checking for query_block info -- look for hash with 'query_block'
      before attempting table/col resolution.  Need special type checking
      for these functions.

=item refactor to common TreeWalker 


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
