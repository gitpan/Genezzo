#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/RCS/Feeble.pm,v 6.2 2004/08/19 21:45:27 claude Exp claude $
#
# copyright (c) 2003, 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Feeble;

use strict;
use warnings;
use Carp;
use Genezzo::Util;
use Term::ReadLine;
use Text::ParseWords; # qw(shellwords);
use Genezzo::Parse::FeebLex;

sub _init
{
    my $self = shift;

    $self->{feeb} = Genezzo::Parse::FeebLex->new();

    return 1;
}

sub new
{
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
    my $self = { };

    return undef
        unless (_init($self, @_));

    return bless $self, $class;

} # end new


# take a ref to an array of strings and subdivide further, preserving
# quoted strings, and separating based upon whitespace and unquoted
# comma's and parentheses.  Search for a specified terminator token in
# the original stream, and honor parenthetical expressions -- the
# terminator must be outside of a right paren.  Leave the terminator
# token in the original word list.
sub nanolex # XXX: obsolete ? 
{
    whoami;
    my $self = shift;

    my ($allwords, $terminator) = @_;

    my $got_term = defined($terminator);

    my $paren_depth = 0;

    my @toklist;

    while (scalar(@{$allwords}))
    {
        my $line = shift @{$allwords};
        next
            unless (defined($line) && length($line));

        if ($got_term)
        {
            # NOTE: if have extra trailing parens (paren depth went
            # negative) then return them.  Client can use paren_depth
            # to decide what to do.

            if (($line =~ m/$terminator/) && (0 >= $paren_depth))
            {
                # put the terminator (e.g. "FROM") back on the list
                unshift @{$allwords}, $line; 
                last;
            }
        }

        # XXX XXX XXX : need to keep track of paren depth 

        # keep the delimiters, ie commas, parens
        foreach my $t1 (parse_line('[,()]', 'delimiters', $line))
        {
            next
                unless ((defined ($t1)) && length($t1));
            $paren_depth++
                if ($t1 =~ m/^\($/ ); # left paren
            $paren_depth--
                if ($t1 =~ m/^\)$/ ); # right paren
            push @toklist, $t1;
        }
    } # end while

    return \@toklist, $paren_depth;

} # end nanolex

sub Parseall
{
    my $self = shift;
    my $line = shift;

    $line =~ s/;(\s*)$//; # XXX : remove the semicolon
    $self->{feeb}->Parseall($line);

    $line =~ s/^\s+//;
    $line =~ s/\s+$//;

    my @prewords = parse_line('\s+', 1, $line);

    my @words;
    for my $ww (@prewords) # clean up a bit
    {
        next
            unless (defined($ww) && length($ww));
        push @words, $ww;
    }

    my ($sql_cmd, $pretty, $badparse);
    my $op = $words[0];
    if ($op =~ /(?i)^(select|insert|delete|update|create|alter)$/)
    {
        my $sqop = "sql_" . $op;

        $sqop = lc $sqop;

        no strict 'refs' ;        
        ($sql_cmd, $pretty, $badparse) =
            &$sqop($self, "", \@words);
    }
    if ($badparse)
    {
        print "\n",$pretty,"\n";
        print ' ' x $badparse, "^\n";
        print ' ' x $badparse, "|\n";
        print '-' x $badparse, "+\n";

        greet $sql_cmd;
    }
    return ($sql_cmd, $pretty, $badparse);
}

sub binops
{
    whoami;
    use overload;
    my @allops;
#    my @bins =  qw(with_assign binary);
    my @bins =  qw(with_assign);
    foreach my $op (split " ", "@overload::ops{ @bins }") 
    {
#        print $op, " ";
        $op = quotemeta($op);
        push @allops, $op;
    }
    @allops = (sort {length($b) <=> length($a)} (@allops));
#    print "\n";
    greet @allops;
}

# parse a select column
sub select_col2
{
#   whoami;
    my $self = shift;

    my ($currhash, $needparen) = @_;

    my $badparse  = 0;
    my ($token, $msg, $msg2);
    my $newtok;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse);
        
        last
            if (($badparse) ||
                !(defined($token)));

#        greet $token;
        
        $currhash->{prettyoffset} = length($self->{feeb}->{pretty});
        $currhash->{type} = $token->{type};

        $currhash->{type} = 'IDENTIFIER'
            if ($currhash->{type} eq 'NONRESERVED_WORD');

        if ($currhash->{type} eq 'RESERVED_WORD')
        {
            if ($token->{val} !~ m/^(min|max|avg|count|sum)$/i )
            {
                my $resword = uc ($token->{val});

                $msg2 = "invalid use of reserved word ($resword)";
#            carp $msg2;
                $badparse = 1;
                next;
            }
        }

        $currhash->{val} = $token->{val};

        # XXX XXX: only accept as standalone token for now...

        if ($token->{val} =~ m/^(min|max|avg|count|sum)$/i )
        {
            $currhash->{type} = "setfn";
            my $newhash = {};
            $currhash->{args} = $newhash;

            ($newtok, $badparse, $msg) = 
                $self->{feeb}->Peek($badparse);

            last
                if (($badparse) ||
                    !(defined($newtok)));

            unless ($newtok->{type} eq 'PAREN_LIST')
            {
                $msg2 = "missing paren list for expression";
                $badparse = 1;
                next;
            }

            ($badparse, $msg2) = 
                $self->select_col2($newhash, 1);

            next
                if $badparse;

            last;
        }

        if ($token->{type} eq 'LPAREN')
        {
            $currhash->{type} = "op";
            $currhash->{child} = [];

            my $got_rparen = 0;

          L_paren:
            while (1)
            {
                ($newtok, $badparse, $msg) = 
                    $self->{feeb}->Peek($badparse);

                last L_paren
                    if (($badparse) ||
                        !(defined($newtok)));

                if ($newtok->{type} eq 'RPAREN')
                {
                    $got_rparen = 1;
                    last L_paren;
                }
                
                my $newhash = {};
                ($badparse, $msg) = 
                    $self->select_col2($newhash);

                last L_paren
                    if ($badparse);                    

                push @{$currhash->{child}}, $newhash;
            }

            if ($got_rparen)
            {
                # remove the right paren
                ($token, $badparse, $msg) = 
                    $self->{feeb}->Pop($badparse);
            }
            else
            {
                $msg2 = "missing right paren for expression";
                $badparse = 1;
                next;
            }

        }
        else
        {
            if ($needparen)
            {
                $msg2 =  "missing paren list for expression";
                $badparse = 1;
                next;

            }
        }

        last;
    }

    $msg = $msg2
        if (defined($msg2));

    return ($badparse, $msg);
}

# parse a select list (between SELECT and FROM)
sub select_list2
{
#   whoami;

    my $self = shift;

    return $self->itemalias_list2("column", @_);
}

# itemalias_list -- both select list columns and from clause table
# list are nearly identical sets of item alias pairs separated by
# commas.  special case a few things (like DISTINCT)
sub itemalias_list2
{
#   whoami;

    my $self = shift;

    # XXX XXX: itemtype only column or table...
    my ($itemtype, $termtoken, $get_one) = @_;

#    greet $termtoken;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my @itemlist;

    my $needalias = 0;
    my $needitem  = 1;

    $get_one = 0 # only get a single item alias pair
        unless (defined($get_one));

    my $currhash;

    my $doPop = 1;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
#            greet $termtoken;
            last ;
        }

        if ($needitem)
        {
            # get $itemtype

            if ($token->{type} eq 'COMMA') # comma match
            {
                ($token, $badparse, $msg) = 
                    $self->{feeb}->Pop($badparse);

                $msg2 = "no $itemtype name";
                $badparse = 1;
                next;
            }

            my ($dstr, $discar) = ($token->{val} =~ m/(?i)^(distinct|all)$/);

            if ($dstr) # distinct | all
            {

                # distinct only valid for column lists
                unless ($itemtype =~ m/^column$/i)
                {
                    ($token, $badparse, $msg) = 
                        $self->{feeb}->Pop($badparse);

                    $dstr = uc $dstr;
                    $msg2 =  "invalid $dstr in $itemtype list";
                    $badparse = 1;
                    next;
                }

                # check if already set distinct or all, or if not
                # first column
                if ((defined($currhash)) || scalar(@itemlist))
                {
                    ($token, $badparse, $msg) = 
                        $self->{feeb}->Pop($badparse);

                    $dstr = uc $dstr;
                    $msg2 = "extra $dstr";
                    $badparse = 1;
                    next;
                }
                else
                {
                    $currhash = {};
                    $currhash->{distinct} = lc $dstr;
                }

            }
            else
            {
                unless (defined($currhash))
                {
                    $currhash = {};
                    $currhash->{distinct} = "all"
                        if ($itemtype =~ m/^column$/i);
                }
                
                # XXX XXX : need to fix for tables
                ($badparse, $msg2) = 
                    $self->select_col2($currhash);

                $doPop = 0;
                $needitem = 0;
            } 
        } # end needitem
        else
        {
            # get comma

            if ($token->{type} eq 'COMMA') # comma match
            {
                if ($needalias)
                {
                    $msg2 = "no alias for $itemtype";
                    $badparse = 1;
                    next;
                }
                else
                {
                    unless (exists($currhash->{name}))
                    {
                        if (exists($currhash->{val}))
                        {
                            $currhash->{name} = $currhash->{val};
                        }
                    }

                    push @itemlist, $currhash
                        if (exists($currhash->{name}));
                    $currhash = ();
                }
                $needitem = 1;

                if ($get_one)
                {
                    if (scalar(@itemlist)) # got one already
                    {
                        $needitem = 0;
                        last;
                    }
                }
                next;
            }
            else # not a comma
            {
                # colname AS colAlias
                if ($token->{val} =~ m/^as$/i)
                {
                    if ($needalias)
                    {
                        $msg2 = "invalid $itemtype AS " ;
                        $msg2 .= $itemtype ."_alias expression";
                        $badparse = 1;
                        $needalias = 0;
                    }
                    else
                    {
                        # valid "AS", so need a col alias
                        $needalias = 1; 
                    }
                    # skip the "AS" token both in normal and failure case
                    next;
                }
                else # colname [as] colAlias - get the alias
                {

                    # check for joins for table
                    if ($itemtype =~ m/^table$/i)
                    {
                        my $jtypes =
                            '^(cross|natural|inner|outer|'
                                . 'union|join|left|right|full)$';         #'

                        if ($token->{val} =~ m/$jtypes/i)
                        {
                            if ($needalias)
                            {
                                $msg2 = "invalid $itemtype AS " ;
                                $msg2 .= $itemtype ."_alias expression";
                                $badparse = 1;
                                next;
                            }

                            my $newhash;

                            ($newhash, $badparse, $msg2) = 
                                $self->join_table2($currhash); 

                            push @itemlist, $newhash
                                unless ($badparse);
                            $currhash = (); # clear the ref to prevent pushing
                            $doPop = 0;
                            next;
                        } # end if possible join
                    } # end table check for join

                    if (exists($currhash->{name}))
                    {
                        my $t1 = $token->{val};
#                        whoami;
                        $msg2 = "($t1) too many names";
                        $badparse = 1;
                        next;
                    }


#                    if ($needalias)
                    {
                        $currhash->{name} = $token->{val};
                        $needalias = 0;
                    }
                
                }
            }
        }
    } # end while

    if ($needalias)
    {
        $msg2 = "invalid $itemtype AS " . $itemtype . "_alias expression";
        # complain twice for "too many names", but do not reset badparse
        
        unless ($badparse)
        {
#            $badparse = 1;
#            ($token, $badparse, $msg) = 
#                $self->{feeb}->Pop($badparse);
            $badparse = (defined($currhash)) ? $currhash->{prettyoffset} : 1;
        }
    }
    else
    {
        unless (exists($currhash->{name}))
        {
            if (exists($currhash->{val}))
            {
                $currhash->{name} = $currhash->{val};
            }
        }

        push @itemlist, $currhash
            if (exists($currhash->{name}));
    }
    # Note: be anal here -- don't allow dangling commas
    if (!$badparse && $needitem)
    {
        $msg2 = "missing $itemtype";
        $msg2 .= " after comma"
            if (scalar(@itemlist));
        $badparse = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse);
    }

    $msg = $msg2
        if (defined($msg2));
    return (\@itemlist, $badparse, $msg);
    
}

# grouporder_list -- both group by list columns and order by 
# list columns are nearly identical sets of col names or refs separated by
# commas.  special case a few things (like ASC|DESC)
sub grouporder_list2
{
#   whoami;

    my $self = shift;

    # XXX XXX: itemtype only group or order...
    my ($itemtype, $termtoken) = @_;

    $itemtype = uc $itemtype;
#    greet $termtoken;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my @itemlist;

    my $needitem  = 1;

    my $currhash;

    my $doPop = 1;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
#            greet $termtoken;
            last ;
        }

        if ($needitem)
        {
            # get $itemtype

            if ($token->{type} eq 'COMMA') # comma match
            {
                ($token, $badparse, $msg) = 
                    $self->{feeb}->Pop($badparse);

                $msg2 = "no $itemtype BY column";
                $badparse = 1;
                next;
            }

            {
                unless (defined($currhash))
                {
                    $currhash = {};
                }
                
                # XXX XXX : need to fix for tables
                ($badparse, $msg2) = 
                    $self->select_col2($currhash);

                $doPop = 0;
                $needitem = 0;
            } 
        } # end needitem
        else
        {
            # get comma

            if ($token->{type} eq 'COMMA') # comma match
            {
                {
                    unless (exists($currhash->{name}))
                    {
                        if (exists($currhash->{val}))
                        {
                            $currhash->{name} = $currhash->{val};
                        }
                    }

                    push @itemlist, $currhash
                        if (exists($currhash->{name}));
                    $currhash = ();
                }
                $needitem = 1;

                next;
            }
            else # not a comma
            {

                my ($collstr, $discar) = 
                    ($token->{val} =~ m/(?i)^(collate|asc|desc)$/);

                if ($collstr) # 
                {
                    # check if already set collation
                    
                }
                else
                {
                    $msg2 = "missing comma";
                    $badparse = 1;
                    next;
                }

            } # end not a comma
        }
    } # end while

    {
        unless (exists($currhash->{name}))
        {
            if (exists($currhash->{val}))
            {
                $currhash->{name} = $currhash->{val};
            }
        }

        push @itemlist, $currhash
            if (exists($currhash->{name}));
    }
    # Note: be anal here -- don't allow dangling commas
    if (!$badparse && $needitem)
    {
        $msg2 = "missing $itemtype by column";
        $msg2 .= " after comma"
            if (scalar(@itemlist));
        $badparse = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse);
    }

    $msg = $msg2
        if (defined($msg2));
    return (\@itemlist, $badparse, $msg);
    
} # end grouporder_list

# create table as select
# create table (col1, col2...) as select
# create table (col1 type, col2 type...)
sub cr_table_clause2
{
#    whoami;
    my $self = shift;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my $termtoken;

    my %crtable_list;

    my $doPop = 1;

    my $curr_list;

    my $tablename;

  L_bigloop:
    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));

        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
            greet $termtoken;
            last ;
        }

        if (!defined($tablename))
        {
            unless ($token->{type} eq 'IDENTIFIER')
            {
                $badparse = 1;
                $msg = "invalid token (" .  $token->{val} 
                . ") for tablename";
                next;
            }
            $tablename = $token->{val};
            $crtable_list{tablename} = $tablename;
        }
        else
        {

            if ($token->{type} eq 'PAREN_LIST')
            {
                my $itemlist;
                ($itemlist, $badparse, $msg) = 
                    $self->ins_list2("create table column list");

                $crtable_list{columns} = $itemlist
                    if (!$badparse);
            }

            last;
        }
    } # end while

#    greet $token, $badparse, $msg;


#    return $_[0]->where_clause2();
    return (\%crtable_list, $badparse, $msg);

} # end cr_table_clause2

sub cr_index_clause2
{
    whoami;
    my $self = shift;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my $termtoken;

    my %crindex_list;

    my $doPop = 1;

    my $curr_list;
    
    my $got_on = 0;

    my ($indexname, $tablename);

  L_bigloop:
    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));

        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
            greet $termtoken;
            last ;
        }

        if (!defined($indexname))
        {
            if ($token->{type} eq 'IDENTIFIER')
            {
                $indexname = $token->{val};
                $crindex_list{indexname} = $indexname;
            }
            else
            {
                $badparse = 1;
                $msg = "invalid token (" .  $token->{val} 
                . ") for index name";
            }
            next;
        }
        
        if (!$got_on)
        {
            unless ($token->{val} =~ m/(?i)^on$/)
            {
                $msg2 = "no ON";
                $badparse = 1;
            }
            $got_on = 1;
            next;
        }

        if (!defined($tablename))
        {
            if ($token->{type} eq 'IDENTIFIER')
            {
                $tablename = $token->{val};
                $crindex_list{tablename} = $tablename;
            }
            else
            {
                $badparse = 1;
                $msg = "invalid token (" .  $token->{val} 
                . ") for tablename";
            }
            next;
            
        }
        else
        {

            if ($token->{type} eq 'PAREN_LIST')
            {
                my $itemlist;
                ($itemlist, $badparse, $msg) = 
                    $self->ins_list2("create index column list");

                $crindex_list{columns} = $itemlist
                    if (!$badparse);
            }

            last;
        }
    } # end while

#    greet $token, $badparse, $msg;

    if (!$badparse)
    {
        if (!defined($indexname))
        {
            $msg2 = "no index name"
                unless (defined($msg2));
            $badparse = 1;
        }
        elsif (!$got_on)
        {
            $msg2 = "no ON"
                unless (defined($msg2));
            $badparse = 1;
        }
        elsif (!defined($tablename))
        {
            $msg2 = "no tablename"
                unless (defined($msg2));
            $badparse = 1;

        }
    }

    $msg = $msg2
        unless (defined($msg));

#    return $_[0]->where_clause2();
    return (\%crindex_list, $badparse, $msg)
}

# parse the alter table add
sub add_list2
{
#    whoami;

    my $self = shift;

    my ($itemtype, $termtoken) = @_;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my $currhash;
    my $add_item;
    my $get_cons;
    my $get_name;
    my $cons_name;

    my $got_last    = 0;
    my $need_first  = 1;
    my $needitem    = 0;
    my $paren_depth = 0; # track paren_depth for parsing 
                         # create table column list

    my $doPop = 1;

    my $bCreateTable = ($itemtype =~ m/create/);

    if ($bCreateTable)
    {
        # skip finding left paren - got it already
        $need_first = 0;
        $needitem   = 1;
    }

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

#        greet $token;

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
#            greet $termtoken;
            last ;
        }

        if (!defined($add_item))
        {
#            greet $token;
            my $cons_thing = "constraint|not|null|check|unique|primary";
            unless ($token->{val} =~ m/^($cons_thing)$/i)
            {
                $badparse = 1;
                $msg = "invalid token (" .  $token->{val} 
                . ") for tablename";
                next;
            }

            $get_cons = 1;

            $add_item = lc($token->{val});
#            greet $add_item;
            $currhash = {};
            $currhash->{type} = "constraint";
            if ($add_item eq "constraint")
            {
                # advance the token if CONSTRAINT - next token should be name
                $get_name = 1;
                next;
            }
            # else fall through (unnamed constraint)
        }
        if ($get_cons)
        {
            if ($get_name && !defined($cons_name))
            {
                unless ($token->{type} eq 'IDENTIFIER')
                {
                    $badparse = 1;
                    $msg = "invalid token (" .  $token->{val} 
                    . ") for constraint name";
                    next;
                }
                $cons_name = $token->{val};
                $currhash->{cons_name} = $cons_name;
            }
            else
            {
#                greet $token;
                unless (exists($currhash->{cons_type}))
                {
                    if ($token->{val} =~ m/^check$/i)
                    {
                        $currhash->{cons_type} = lc($token->{val});
                        my $where_clause;

                        # Note: treat CHECK constraint as WHERE clause
                        ($where_clause, $badparse, $msg)  
                            = $self->where_clause2();
                        if (!$badparse && (defined($where_clause)))
                        {
                            $currhash->{where_clause} = $where_clause;
                        }
                        last;
                    }
                    elsif ($token->{val} =~ m/^(unique|primary)$/i)
                    {
                        my $ct1 = ($token->{val} =~ m/^unique$/i) ?
                            "unique" : "primary_key";

                        $currhash->{cons_type} = $ct1;

                        if ($currhash->{cons_type} eq "primary_key")
                        {
                            my $next_token;
                            ($next_token, $badparse, $msg) = 
                                $self->{feeb}->Peek($badparse, 2)
                                unless (($badparse) ||
                                        !(defined($token)));
                            last
                                if ($badparse); 

                            if (!(defined($next_token)))
                            {
                                $msg = "missing KEY";
                                $badparse = 1;
                                next;
                            }
                            
                            # look for key
                            greet $next_token;

                            unless ($next_token->{val} =~ m/^key$/i)
                            {
                                $msg = "missing KEY";
                                $badparse = 1;
                                next;
                            }
                            ($token, $badparse, $msg) = 
                                $self->{feeb}->Pop($badparse);
                            
                            next # error check
                                if (($badparse) ||
                                    !(defined($token)));

                            ($token, $badparse, $msg) = 
                                $self->{feeb}->Peek($badparse);
                        }

                        my $col_list;

                        # Note: UNIQUE column list same as insert,
                        # create table
                        ($col_list, $badparse, $msg)  =
                            $self->ins_list2("$ct1 constraint column list");
                        if (!$badparse && (defined($col_list)))
                        {
                            $currhash->{col_list} = $col_list;
                        }
                        last;
                    }
                    else
                    {
                        $badparse = 1;
                        $msg = "invalid token (" .  $token->{val} 
                        . ") for tablename";
                        next;
                    }
                }
            }
        } # end if get cons
        else
        {
            $badparse = 1;
            $msg = "invalid token (" .  $token->{val} 
            . ") for ALTER TABLE ... ADD";
            next;
        }

    } # end while

#    greet $badparse, $add_item, @itemlist;

    # Note: be anal here -- don't allow dangling commas
    if (!$badparse && !defined($add_item))
    {
        $msg2 = "missing add type";
        $badparse = 1;
    }

#    greet @itemlist;
    $msg = $msg2
        if (defined($msg2));

    # Note: empty itemlist is okay
    return ($currhash, $badparse, $msg);
    
} # end add_list


sub alt_table_clause2
{
#    whoami;
    my $self = shift;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my $termtoken;

    my %alt_table_list;

    my $doPop = 1;

    my $curr_list;

    my $tablename;

  L_bigloop:
    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));

        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
            greet $termtoken;
            last ;
        }

        if (!defined($tablename))
        {
            unless ($token->{type} eq 'IDENTIFIER')
            {
                $badparse = 1;
                $msg = "invalid token (" .  $token->{val} 
                . ") for tablename";
                next;
            }
            $tablename = $token->{val};
            $alt_table_list{tablename} = $tablename;
        }
        else
        {

            if ($token->{val} =~ m/^add$/i)
            {
                my $itemlist;
                ($itemlist, $badparse, $msg) = 
                    $self->add_list2("alter table add list");

                $alt_table_list{add_list} = $itemlist
                    if (!$badparse);
            }

            last;
        }
    } # end while

#    greet $token, $badparse, $msg, %alt_table_list;


#    return $_[0]->where_clause2();
    return (\%alt_table_list, $badparse, $msg);

} # end alt_table_clause2


# main CREATE parsing
sub sql_create
{
#   whoami;
    my $self = shift;
    
    my ($pretty, $allwords) = @_;
    my %create_clause;
    
    my $operation = shift @{$allwords};
    
    my $badparse;
    
    $pretty .= "$operation ";
    
    my $sel_list;
    
    my $toknum = 0;
    my ($token, $msg, $msg2, $termtoken);

    my $doPop = 1;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
#            greet $termtoken;
            last ;
        }

        if (exists($create_clause{operation}))
        {
            my $t1 = $token->{val};
            $msg2 = "invalid token ($t1) after CREATE";
            $badparse = 1;

            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);

            next;
        }
        else
        {
            my $t1 = '^(table|index)$';
            
            unless ($token->{val} =~ m/(?i)($t1)/)
            {
                $msg = "invalid CREATE operation (".
                    $token->{val} . ")";
                $badparse = 1;
                last;
            }
            $create_clause{operation} = $token->{val};
        }
        last;
    } # end while

    unless (exists($create_clause{operation}))
    {
        unless ($badparse)
        {
            $badparse = 1;

            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);

            $msg2 = "no operation";
        }
    }

   unless ($badparse)
   {
       my $clausename = "cr_" . lc($create_clause{operation});
       my $clause_list;
       my $clause_op =  $clausename . '_clause2';

       no strict 'refs' ;     
       ($clause_list, $badparse, $msg) =    
           &$clause_op($self);

       $create_clause{$clausename . "_list"} = $clause_list;
   }

    $msg = $msg2
        if (defined($msg2));

    if ($badparse)
    {
        $badparse = 0;
        while (!$badparse)
        {
            ($token, $badparse, $msg2) = 
                $self->{feeb}->Pop($badparse);

            last
                unless (defined($token));
        }
        
        $pretty = $self->{feeb}->{pretty};
        $badparse = $self->{feeb}->{badparse};
        
        carp $msg;
    }

    return (\%create_clause, $pretty, $badparse);
} # end sql_create

# main ALTER parsing
sub sql_alter
{
#   whoami;
    my $self = shift;
    
    my ($pretty, $allwords) = @_;
    my %alter_clause;
    
    my $operation = shift @{$allwords};
    
    my $badparse;
    
    $pretty .= "$operation ";
    
    my $sel_list;
    
    my $toknum = 0;
    my ($token, $msg, $msg2, $termtoken);

    my $doPop = 1;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
#            greet $termtoken;
            last ;
        }

        if (exists($alter_clause{operation}))
        {
            my $t1 = $token->{val};
            $msg2 = "invalid token ($t1) after ALTER";
            $badparse = 1;

            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);

            next;
        }
        else
        {
            my $t1 = '^(table|index)$';
            
            unless ($token->{val} =~ m/(?i)($t1)/)
            {
                $msg = "invalid ALTER operation (".
                    $token->{val} . ")";
                $badparse = 1;
                last;
            }
            $alter_clause{operation} = $token->{val};
        }
        last;
    } # end while

    unless (exists($alter_clause{operation}))
    {
        unless ($badparse)
        {
            $badparse = 1;

            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);

            $msg2 = "no operation";
        }
    }

   unless ($badparse)
   {
       my $clausename = "alt_" . lc($alter_clause{operation});
       my $clause_list;
       my $clause_op =  $clausename . '_clause2';

       no strict 'refs' ;     
       ($clause_list, $badparse, $msg) =    
           &$clause_op($self);

       $alter_clause{$clausename . "_list"} = $clause_list;
   }

    $msg = $msg2
        if (defined($msg2));

    if ($badparse)
    {
        $badparse = 0;
        while (!$badparse)
        {
            ($token, $badparse, $msg2) = 
                $self->{feeb}->Pop($badparse);

            last
                unless (defined($token));
        }
        
        $pretty = $self->{feeb}->{pretty};
        $badparse = $self->{feeb}->{badparse};
        
        carp $msg;
    }

    return (\%alter_clause, $pretty, $badparse);
} # end sql_alter

sub update_list
{
    whoami;
    my $self = shift;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my $termtoken = '(?i)^where$';              #'

    my @upd_list;

    my $got_where  = 0;

    my $doPop = 1;

    my $ustate = 1; # 1 = get cols, 2 = get equals, 3 = get expre

    my $curr_list;

  L_bigloop:
    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{type} ne 'PAREN_LIST')
            && ($token->{val} =~ /$termtoken/))
        {
            greet $termtoken;
            $got_where = 1;
            last ;
        }

        greet $token;

        if (1 == $ustate)
        {
        
            if (defined($curr_list) && scalar(@{$curr_list}))
            {
                push @upd_list, $curr_list;
            }

            $curr_list = [];

            if ($token->{type} eq 'PAREN_LIST')
            {
                my ($foo, $badparse, $msg) = $self->update_cols();
                greet $foo;

                if ($badparse)
                {
                    last;
                }
                push @{$curr_list}, $foo;
                $ustate++;
            }
            else
            {
                greet $token->{val};
                my $vv = $token->{val};
                if ($token->{type} eq 'IDENTIFIER') # normal case
                {
                    push @{$curr_list}, $vv;
                    $ustate++;
                }
                else
                {
                    $msg = "invalid token: " . $token->{val};
                    $badparse = 1;
                    last L_bigloop;
                }
            }
            next;
        }
        elsif (2 == $ustate)
        {
            my $vv = $token->{val};

            if ($token->{val} eq '=')
            {
                $ustate++;
                next;
            }
            else
            {
                $msg =  "missing EQUALs!";
                $badparse = 1;
                last;
            }
        }
        elsif (3 == $ustate)
        {
            if ($token->{type} eq 'PAREN_LIST')
            {
                greet $token;
                my $foo;
                ($foo, $badparse, $msg) = $self->update_expr();
                greet $foo;
                push @{$curr_list}, $foo;
                $ustate++;
            }
            elsif ($token->{type} eq 'COMMA')
            {
                $msg =  "missing expression after EQUALs";
                $badparse = 1;
                last;
            }
            else
            {
                my $vv = $token->{val};
                greet $token->{val};

                if ($vv =~ m/^=/) # shouldn't have a leading ='s
                {

                    # might happen for tokens like "a==5".  The colval
                    # split would create a new token "=5" and skip
                    # equal's processing

                    $msg =  "invalid token: " . $token->{val};
                    $badparse = 1;
                    last L_bigloop;
                }

                push @{$curr_list}, $token->{val};
                $ustate++;
            }
            next;
        }
        elsif (4 == $ustate)
        {
            if ($token->{type} eq 'COMMA')
            {
                $ustate = 1;
                next;
            }
            else
            {
#                greet "no comma!";
#                $badparse = 1;
#                last;

                # keep appending expression
                $curr_list->[-1] .= " " . $token->{val};
            }

        }
        else
        {
            $msg = "what the heck!";
            $badparse = 1;
            last;
        }
#        last;

    } # end while

    my $arysize = (defined($curr_list)) ? scalar(@{$curr_list}) : 0;

    unless ($badparse)
    {
        if (0 == $arysize)
        {
            $msg = "missing column after comma";

            $msg = "missing column expression after SET"
                unless (scalar(@upd_list));

            $badparse = 1;
        }
        elsif (1 == $arysize)
        {
            $msg = "missing column expression";

            $msg = "missing expression after equals"
                if ($ustate > 2);

            $badparse = 1;
        }
        elsif ((2 == $arysize)
               && (1 == $ustate))
        {
            $msg = "missing column after comma";
            $badparse = 1;
        }
    }
    push @upd_list, $curr_list
        if ($arysize && !$badparse);

    return (\@upd_list, $badparse, $msg);

} # end update_list

sub update_cols
{
   whoami;

    my $self = shift;

    my $termtoken = shift;

#    greet $termtoken;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my $itemtype = "column";
    my @itemlist;

    my $needalias = 0;
    my $needitem  = 1;

    my $currhash;

    my $doPop = 1;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
            greet $termtoken;
            last ;
        }
        if ($token->{type} eq 'RPAREN')
        {
            greet $token;
            last ;
        }

#        greet $token;

        if ($needitem)
        {
            # get $itemtype

            if ($token->{type} eq 'COMMA') # comma match
            {
                ($token, $badparse, $msg) = 
                    $self->{feeb}->Pop($badparse);

                $msg2 = "no $itemtype name";
                $badparse = 1;
                next;
            }

#                ($badparse, $msg2) = 
#                    $self->select_col2($currhash);

            $currhash->{name} = $token->{val};

#                $doPop = 0;
                $needitem = 0;
        } # end needitem
        else
        {
            # get comma

            if ($token->{type} eq 'COMMA') # comma match
            {
                if ($needalias)
                {
                    $msg2 = "no alias for $itemtype";
                    $badparse = 1;
                    next;
                }
                else
                {
                    unless (exists($currhash->{name}))
                    {
                        if (exists($currhash->{val}))
                        {
                            $currhash->{name} = $currhash->{val};
                        }
                    }

                    push @itemlist, $currhash
                        if (exists($currhash->{name}));
                    $currhash = ();
                }
                $needitem = 1;

                next;
            }
            else # not a comma
            {
                # colname AS colAlias
                if ($token->{val} =~ m/^as$/i)
                {
                    if ($needalias)
                    {
                        $msg2 = "invalid $itemtype AS " ;
                        $msg2 .= $itemtype ."_alias expression";
                        $badparse = 1;
                        $needalias = 0;
                    }
                    else
                    {
                        # valid "AS", so need a col alias
                        $needalias = 1; 
                    }
                    # skip the "AS" token both in normal and failure case
                    next;
                }
                else # colname [as] colAlias - get the alias
                {

                    if (exists($currhash->{name}))
                    {
                        my $t1 = $token->{val};
#                        whoami;
                        $msg2 = "($t1) too many names";
                        $badparse = 1;
                        next;
                    }


#                    if ($needalias)
                    {
                        $currhash->{name} = $token->{val};
                        $needalias = 0;
                    }
                
                }
            }
        }
    } # end while

    if ($needalias)
    {
        $msg2 = "invalid $itemtype AS " . $itemtype . "_alias expression";
        # complain twice for "too many names", but do not reset badparse
        
        unless ($badparse)
        {
#            $badparse = 1;
#            ($token, $badparse, $msg) = 
#                $self->{feeb}->Pop($badparse);
            $badparse = (defined($currhash)) ? $currhash->{prettyoffset} : 1;
        }
    }
    else
    {
        unless (exists($currhash->{name}))
        {
            if (exists($currhash->{val}))
            {
                $currhash->{name} = $currhash->{val};
            }
        }

        push @itemlist, $currhash
            if (exists($currhash->{name}));
    }
    # Note: be anal here -- don't allow dangling commas
    if (!$badparse && $needitem)
    {
        $msg2 = "missing $itemtype";
        $msg2 .= " after comma"
            if (scalar(@itemlist));
        $badparse = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse);
    }

    $msg = $msg2
        if (defined($msg2));
    return (\@itemlist, $badparse, $msg);
    
} # end update_cols

sub update_expr
{
    whoami;

    return $_[0]->where_clause2();
}

# main UPDATE parsing
sub sql_update
{
    whoami;
    my $self = shift;

    my ($pretty, $allwords) = @_;

    my $operation = shift @{$allwords};

    my $badparse = 0;
    my ($msg, $msg2, $token);

#    my $termtoken = '(?i)^where$';              #'
    my $termtoken = '(?i)^set$';              #'

    my ($upd_list, %upd_clause);

    my $got_set   = 0;

    my $doPop = 1;

    while (1)
    {
        my $tablename;
        my $alias;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
#            greet $termtoken;
            $got_set = 1;
            last ;
        }

#        greet $token;

        if (exists($upd_clause{alias}))
        {
            my $t1 = $token->{val};
            $msg2 = "invalid token ($t1) after table alias";
            $badparse = 1;

            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);

            next;
        }
        elsif (exists($upd_clause{tablename}))
        {
            $alias = $upd_clause{alias} = $token->{val};
        }
        else
        {
            $tablename = $upd_clause{tablename} = $token->{val};
        }
#        last;
    } # end while

    unless (exists($upd_clause{tablename}) && $got_set)
    {
        unless ($badparse)
        {
            $badparse = 1;

            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);
            
            if (exists($upd_clause{tablename}))
            {
                $msg2 = "no SET";
            }
            else
            {
                $msg2 = "no tablename";
            }
        }
    }

    for my $i (1..2)
    {
        $msg = $msg2
            if (defined($msg2));

        if ($badparse)
        {
            $pretty = $self->{feeb}->{pretty};
            $badparse = $self->{feeb}->{badparse} ?
                $self->{feeb}->{badparse} : length($pretty);
        
            carp $msg;

            return (\%upd_clause, $pretty, $badparse);
        }

        last
            if ($i > 1);


        unless ($badparse)
        {
            my $clausename = "update";
            my $clause_list;
            my $clause_op = $clausename . '_list';

            no strict 'refs' ;     
            ($clause_list, $badparse, $msg) =    
                &$clause_op($self);

            $upd_clause{$clausename . "_list"} = $clause_list;
        }

        unless ($badparse)
        {
            my $clausename = "where";
            my $clause_list;
            my $clause_op = $clausename . '_clause2';

            no strict 'refs' ;     
            ($clause_list, $badparse, $msg) =    
                &$clause_op($self);

            $upd_clause{$clausename . "_list"} = $clause_list;
        }
    }

    $msg = $msg2
        if (defined($msg2));

    return (\%upd_clause, $pretty, $badparse);

} # end sql_update

# main DELETE parsing
sub sql_delete
{
#   whoami;
    my $self = shift;

    my ($pretty, $allwords) = @_;

    my $operation = shift @{$allwords};

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my $termtoken = '(?i)^where$';              #'

    my ($del_list, %del_clause);

    my $got_from   = 0;

    my $doPop = 1;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
#            greet $termtoken;
            last ;
        }

        if (!$got_from)
        {
            unless ($token->{val} =~ m/(?i)^from$/)
            {
                $msg2 = "no FROM";
                $badparse = 1;
            }
            $got_from = 1;
            next;
        }

        if (exists($del_clause{tablename}))
        {
            my $t1 = $token->{val};
            $msg2 = "invalid token ($t1) after tablename";
            $badparse = 1;

            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);

            next;
        }
        else
        {
            $del_clause{tablename} = $token->{val};
        }
#        last;
    } # end while

    unless (exists($del_clause{tablename}))
    {
        unless ($badparse)
        {
            $badparse = 1;

            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);

            $msg2 = "no tablename";
        }
    }

    for my $i (1..2)
    {
        $msg = $msg2
            if (defined($msg2));

        if ($badparse)
        {
            $pretty = $self->{feeb}->{pretty};
            $badparse = $self->{feeb}->{badparse} ?
                $self->{feeb}->{badparse} : length($pretty);
        
            carp $msg;

            return (\%del_clause, $pretty, $badparse);
        }

        last
            if ($i > 1);

        return (\%del_clause,  $pretty, $badparse)
            unless (defined($token));

        {
            my $clausename = "where";
            my $clause_list;
            my $clause_op = $clausename . '_clause2';

            no strict 'refs' ;     
            ($clause_list, $badparse, $msg) =    
                &$clause_op($self);

            $del_clause{$clausename . "_list"} = $clause_list;
        }
    }

    $msg = $msg2
        if (defined($msg2));

    return (\%del_clause, $pretty, $badparse);

} # end sql_delete

# parse the insert columns
sub ins_list2
{
#   whoami;

    my $self = shift;

    my ($itemtype, $termtoken) = @_;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my @itemlist;

    my $got_last    = 0;
    my $need_first  = 1;
    my $needitem    = 0;
    my $paren_depth = 0; # track paren_depth for parsing 
                         # create table column list

    my $doPop = 1;

    my $bCreateTable = ($itemtype =~ m/create/);

    if ($bCreateTable)
    {
        # skip finding left paren - got it already
        $need_first = 0;
        $needitem   = 1;
    }

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

#        greet $token;

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
#            greet $termtoken;
            last ;
        }

        if ($got_last) # should never get here!!
        {
            $msg2 = "extra tokens after rparen";
            $badparse = 1;
            next;
        }

        if ($need_first)
        {
            $need_first = 0;
            $needitem = 1;

            unless ($token->{type} eq 'PAREN_LIST')
            {
                greet $token;
                $msg2 = "no left paren";
                $badparse = 1;
                $needitem = 0;
            }
            next;
        }

        if ($needitem)
        {
            # get $itemtype

            if ($token->{type} eq 'COMMA')
            {
                $msg2 = "no $itemtype";
                $badparse = 1;
                next;
            }

            if ($bCreateTable)
            {
                if ($token->{type} ne 'IDENTIFIER')
                {
                    # might be nonreserved vs reserved...
#                    $msg2 = "invalid use of reserved word (" .
#                        uc($token->{val}) . ") for column name";
                    $msg2 = "column name (" .
                        $token->{val} . ") must be an identifier";
                    $badparse = 1;
                    next;
                }

                my $foo = {name => $token->{val}};
                push @itemlist, $foo;
            }
            else
            {
                push @itemlist, $token->{val};
            }

            $needitem = 0;

        } # end needitem
        else
        {
            # get comma

            if (!$paren_depth && ($token->{type} eq 'COMMA'))
            {
                $needitem = 1;
            }
            elsif (!$paren_depth && ($token->{type} eq 'RPAREN'))
            {
                $needitem = 0;
                $got_last = 1;
            }
            else
            {
                unless ($bCreateTable)
                {
                    $msg = "unknown token (" . $token->{val} 
                    . ") in $itemtype list";
                    $badparse = 1;
                    next;
                }

                # Note: column types are optional in create table
                # because they may get supplied by create table as
                # select.  Need to process other stuff here like
                # constraints, default values, etc
                my $foo = $itemlist[-1];
                if (exists($foo->{type}))
                {
#                    $msg = "unknown token (" . $token->{val} 
#                    . ") in $itemtype list";
#                    $badparse = 1;
#                    next;
                    
                    $paren_depth--
                        if ($token->{type} eq 'RPAREN');

                    unless ($paren_depth >= 0)
                    {
                        $msg = "too many right parentheses";
                        $badparse = 1;
                        next;
                    }

                    my $tval = $token->{val};

                    if ($token->{type} eq 'PAREN_LIST')
                    {
                        $paren_depth++;
                        $tval = '('; # LPAREN
                    }

                    if (exists($foo->{other}))
                    {
                        $itemlist[-1]->{other} .= " " . $tval;
                    }
                    else
                    {
                        $itemlist[-1]->{other} = $tval;
                    }

                }
                else
                {
                    $itemlist[-1]->{type} = $token->{val};
                }
            }
        }
    } # end while

    # Note: be anal here -- don't allow dangling commas
    if (!$badparse && $needitem)
    {
        $msg2 = "missing $itemtype";
        $msg2 .= " after comma"
            if (scalar(@itemlist));
        $badparse = 1;
    }
    if (!$badparse && !$got_last)
    {
        # need to terminate paren if have items
        if (scalar(@itemlist))
        {
            $msg2 = "missing rparen";
            $badparse = 1;
        }
    }

#    greet @itemlist;
    $msg = $msg2
        if (defined($msg2));

    # Note: empty itemlist is okay
    return (\@itemlist, $badparse, $msg);
    
} # end ins_list

# main INSERT parsing
sub sql_insert
{
#   whoami;
    my $self = shift;

    my ($pretty, $allwords) = @_;

    my $operation = shift @{$allwords};

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my $termtoken = '(?i)(^values$)|(^select$)';              #'

    my ($ins_list, %ins_clause);

    my $got_into   = 0;

    my $doPop = 1;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
#            greet $termtoken;
            last ;
        }

        if (!$got_into)
        {
            unless ($token->{val} =~ m/(?i)^into$/)
            {
                $msg2 = "no INTO";
                $badparse = 1;
            }
            $got_into = 1;
            next;
        }

        $ins_clause{tablename} = $token->{val};
        last;
    } # end while


    if (!$badparse && (exists($ins_clause{tablename})))
    {
        my $collist;
        ($collist, $badparse, $msg2) = 
            $self->ins_list2('column name', $termtoken);

        $ins_clause{colnames} = $collist;
    }
    else
    {
        if (!$got_into)
        {
            $msg2 = "no INTO";
        }
        else
        {
            $msg2 = "no tablename";
        }
        $badparse = 1;
    }

    for my $i (1..2)
    {
        $msg = $msg2
            if (defined($msg2));

        if ($badparse)
        {
            $pretty = $self->{feeb}->{pretty};
            $badparse = $self->{feeb}->{badparse} ?
                $self->{feeb}->{badparse} : length($pretty);
            
            carp $msg;

            return (\%ins_clause, $pretty, $badparse);
        }

        last
            if ($i > 1);

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse);

        next
            if ($badparse);

        last
            if (defined($token));
    }

    return (\%ins_clause, $pretty, $badparse)
        unless (defined($token));

    
    if ($token->{val} =~ m/(?i)^select$/)
    {
        my $selclause;
        ($selclause, $pretty, $badparse) = 
            $self->sql_select($pretty, ["SELECT"]);
        $ins_clause{selclause} = $selclause;
    }
    else 
    {
        unless ($token->{val} =~ m/(?i)^values$/)
        {
            $msg2 = "no VALUES clause";
            $badparse = 1;
            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);
        }
    }
    unless ($badparse)
    {
        my $colvals;
        ($colvals, $badparse, $msg) = 
            $self->ins_list2('column values');
        $ins_clause{colvals} = $colvals;
    }

    if ($badparse)
    {
        $pretty = $self->{feeb}->{pretty};
            $badparse = $self->{feeb}->{badparse} ?
                $self->{feeb}->{badparse} : length($pretty);
            
        carp $msg;
    }
    return (\%ins_clause, $pretty, $badparse);

}

# main SELECT parsing
sub sql_select
{
#   whoami;
    my $self = shift;
    
    my ($pretty, $allwords) = @_;
    my %selclause;
    
    my $operation = shift @{$allwords};
    
    my $badparse;
    
    $pretty .= "$operation ";
    
    my $sel_list;
    
    my $toknum = 0;
    my ($token, $msg);
    
    ($token, $badparse, $msg, $toknum) = 
        $self->{feeb}->Seek($badparse, '(?i)(^from$)');
    
#        greet $token, $badparse;
    
    if ($badparse)
    {
        $badparse = 0;
        while (!$badparse)
        {
            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);
        }
        
        $pretty = $self->{feeb}->{pretty};
        $badparse = $self->{feeb}->{badparse};
        
        carp $msg;
        return (\%selclause, $pretty, $badparse);
    }
    
    unless (defined($token)) # null token
    {
        while (1)
        {
            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);
            
            last
                unless (defined($token));
            
            last
                if $badparse;
            
#                    greet $token;
            
            my $frm1 = '(?i)(^from$)'; 
            my $frm2 = '^(frm|form|fro(.?)|(.?)f(.?)r(.?)o(.?)m(.?))$'; #'
            
            if ($token->{val} =~ m/$frm1/)
            {
                $badparse = 1;
                next;
            }
            if ($token->{val} =~ m/$frm2/i)
            {
                $badparse = 1;
                next;
            }
        }
        
        carp "missing FROM clause or invalid FROM location";
        $pretty = $self->{feeb}->{pretty};
        $badparse = (($badparse) ? 
                     $self->{feeb}->{badparse} :
                     length($pretty));
        
        return (\%selclause, $pretty, $badparse);
    } # end unless !null token
    
    my %gotclause;
    
    my $prevclause = 'from';
    
    $gotclause{$prevclause} = undef;
    
    
    # a select statement is of the general form 
    #
    # SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY
    #
    # we've parsed the select list and have everything from FROM
    # onward.  Build a hash of FROM, WHERE, etc clauses.  
    #
    # start with first "marker" token as FROM, and use second "marker"
    # WHERE as terminator for the seek.  If remainder of token stream
    # starts with terminator WHERE, then we know the FROM clause is
    # complete.  If where isn't found then advance to next marker
    # GROUP and use it as the terminator.  When the terminator,
    # e.g. GROUP, is found, it become the first marker 
    #
    #  NOTE: "ORDER BY" is really associated with a compound query
    #  expression, not an individual SELECT statement.
    #
    
    for my $marker qw(where group having order)
    {
        # build a case-insensitive terminator
        my $termexp = '(?i)(^' . $marker . '$)';
        my $marker_toknum;
        
        ($token, $badparse, $msg, $marker_toknum) = 
            $self->{feeb}->Seek($badparse, $termexp, 0, $toknum);
        
        last
            if ($badparse);
        
        if (defined($token))
        {
            $gotclause{$prevclause} = $marker;
            $gotclause{$marker} = undef;
            $prevclause = $marker;
            
            $toknum = $marker_toknum;
            $toknum++;
        }
    }
    
    if ($badparse)
    {
        $badparse = 0;
        while (!$badparse)
        {
            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);
        }
        
        $pretty = $self->{feeb}->{pretty};
        $badparse = $self->{feeb}->{badparse};
        
        carp $msg;
        return (\%selclause, $pretty, $badparse);
    }
    
    my $termexp;
    
    $termexp = '(?i)(^' . "from" . '$)';
    
    ($sel_list, $badparse, $msg) = 
        $self->select_list2($termexp);
    
#        greet $sel_list;
    $selclause{sql_command} = $operation;
    $selclause{select_list} = $sel_list;
    
    if ($badparse)
    {
        
        $pretty = $self->{feeb}->{pretty};
        $badparse = $self->{feeb}->{badparse};
        
        carp $msg;
        return (\%selclause, $pretty, $badparse);
    }
    
    for my $clausename qw(from where group having order)
    {
        next
            unless (exists($gotclause{$clausename}));
        
        my $vv = $gotclause{$clausename};
        
        if (1)
        {
            my $clause_list;
            my $clause_op = $clausename . '_clause2';
            
            my $termexp = (defined($vv)) ?
                ('(?i)(^' . $vv . '$)')
                    : undef;
            
            no strict 'refs' ;     
            ($clause_list, $badparse, $msg) =    
                &$clause_op($self, $termexp);
            
            $selclause{$clausename . "_list"} = $clause_list;
            
            if ($badparse)
            {
                
                $pretty = $self->{feeb}->{pretty};
                $badparse = $self->{feeb}->{badparse};
                
                carp $msg;
                return (\%selclause, $pretty, $badparse);
            }
            
        }
    } # end for

    # XXX XXX XXX XXX XXX XXX
    #greet %selclause;
    
    return (\%selclause, $pretty, $badparse);

    #    select "a from, from",b,c from emp
    #  select "a from, from" "baz boo",b d,e,f g,"h i j" "k l m" from emp
    # select avg ( "foo", ( max ( "emp", "baz, sdf )" )) "))" ) from emp

#    greet @params;

} # end sql_select

sub nothing_clause2
{
#    whoami;
    my $self = shift;
    my $termtoken = shift;

    my ($msg, $msg2, $token);
    my $badparse = 0;
    my @funky;

    my $doPop = 1;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

#        greet $token->{val};

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
            greet $termtoken;
            last ;
        }
    }

#    $badparse = 0;

    return (\@funky, $badparse, $msg);
}


sub from_clause2
{
#   whoami;
    my $self = shift;

    return $self->itemalias_list2("table", @_);
}

sub where_clause2
{
#   whoami;
    my $self = shift;
#    greet $self->{feeb};

    my $termtoken = shift;

    my $badparse = 0;
    my ($msg, $msg2, $token);

    my @itemlist;
                 
    my $doPop = 1;
    my $getwhitespace = 0;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse, $getwhitespace)# XXX: get whitespace
                if ($doPop);

        $doPop = 1;
#        $getwhitespace = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse, $getwhitespace)# XXX: get whitespace
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

#        greet $token->{val};

        if ((defined ($termtoken))
            && ($token->{val} =~ /$termtoken/))
        {
            greet $termtoken;
            last ;
        }
        my $newhash = {};

        # XXX XXX: flatten out the paren_lists for now.
        # Need to treat this more intelligently...
        if ($token->{type} eq 'PAREN_LIST')
        {
            # XXX XXX: should be LPAREN
            unless (scalar(@{$token->{val}}))
            {
                $msg = "bad paren list";
                $badparse = 1;
                last;
            }
            my $h2 = $token->{val}->[0];
            $newhash->{val}  = $h2->{val};
            $newhash->{type} = $h2->{type};
        }
        else
        {
            $newhash->{val}  = $token->{val};
            $newhash->{type} = $token->{type};

        }
        push @itemlist, $newhash;
    }

#    $badparse = 0;

    return (\@itemlist, $badparse, $msg);
}

sub having_clause2
{
#   whoami;
    my $self = shift;
    return $self->nothing_clause2(@_);
}

sub by_thing
{
#   whoami;
    my $self = shift;
    my $item_type = shift;

    my ($msg, $msg2, $token);
    my $badparse;
    my @funky;

    my $doPop = 1;

    my $gotBy = 0;

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));
        last
            if (($badparse) ||
                !(defined($token)));

        if ($token->{val} =~ m/^by$/i)
        {
            $gotBy = 1;
        }
        else
        {
            my $tt = $token->{val};
            $msg2 = "invalid token ($tt)";
            # pop the token to set badparse position correctly
            ($token, $badparse, $msg) = 
                $self->{feeb}->Pop($badparse);
            $badparse = 1;
            next;
        }

        last;
    }

    if (!$gotBy && !$badparse)
    {
        $msg2 = "no BY";
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse);
        $badparse = 1;
    }

    $msg = $msg2
        if (defined($msg2));

    return (\@funky, $badparse, $msg)
        if ($badparse);

    return $self->grouporder_list2($item_type, @_);
}

sub group_clause2
{
#   whoami;
    my $self = shift;

    return $self->by_thing("group", @_);
}
sub order_clause2
{
#   whoami;
    my $self = shift;

    return $self->by_thing("order", @_);
}

sub join_table2
{
#   whoami;
    my $self = shift;

    my ($currhash) = @_;

    my $badparse  = 0;
    my ($token, $msg, $msg2);
    my $newtok;

    my $doPop = 0;
    my $gotJoin = 0;
    my $newhash = {};

    $token = 1; # define token to fix peek

    while (1)
    {
        ($token, $badparse, $msg) = 
            $self->{feeb}->Pop($badparse)
                if ($doPop);

        $doPop = 1;

        ($token, $badparse, $msg) = 
            $self->{feeb}->Peek($badparse)
                unless (($badparse) ||
                        !(defined($token)));

        last
            if (($badparse) ||
                !(defined($token)));

        if (!$gotJoin)
        {

            my $jtype = uc ($token->{val});

            if ($jtype =~ m/^join$/i)
            {
                $doPop   = 0; # let itemalias pop this token
                $gotJoin = 1;
            }

            # XXX XXX : need to figure out valid join types

            if (exists($newhash->{type}))
            {
                $newhash->{type} .= " $jtype";
            }
            else
            {
                $newhash->{type} = $jtype;
            }
            next;
        }

#        greet $token, $self->{feeb};

        push @{$newhash->{child}}, $currhash;
        my $tab_list;
        # Note: get a single table
        ($tab_list, $badparse, $msg2) =  # use get_one arg

            #                               termtoken, getone
            $self->itemalias_list2("table",  undef, 1);

        push @{$newhash->{child}}, @{$tab_list}
            unless ($badparse);

        last;
    }

    if (!$gotJoin)
    {
        $msg2 = "no JOIN";
        $badparse = 1;
    }

    $msg = $msg2
        if (defined($msg2));

    return ($newhash, $badparse, $msg);
}


sub c
{ # create
    my $self = shift;

    my ($operation, @params) = @_;

    unless (@params)
    {
        whisper "warn: no params";
        return 0;
    }

    my @validops = qw(
                      table
                      tablespace
                      );

    my $subop = checkKeyVal(kvpair => $params[0],
                            validlist => \@validops);

    unless (defined($subop))
    {
        whisper "warn: bad create option";
        return 0;
    }


} # end create


END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE


1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Feeble.pm - a feeble parser

=head1 SYNOPSIS

=head1 DESCRIPTION

  The Feeble parser parses a (very) limited subset of SQL.

=head1 ARGUMENTS

=head1 FUNCTIONS

=head2 EXPORT

=head1 LIMITATIONS

many

=head1 TODO

=over 4

=item Use antlr (see antlr.org) to generate a parser, and toss this code.

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
