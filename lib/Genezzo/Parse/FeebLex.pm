#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Parse/RCS/FeebLex.pm,v 6.2 2004/10/04 07:58:38 claude Exp claude $
#
# copyright (c) 2003, 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Parse::FeebLex;

use strict;
use warnings;
use Carp;
use Genezzo::Util;
use Text::ParseWords; # qw(shellwords);
#use Term::ANSIColor qw(colored);

our (%tokentype, %reloptype, @relopary);

BEGIN 
{
# XXX XXX: should end exec have a dash or underscore?
#       ELSE   END   END-EXEC   ESCAPE

my @reserved_word = 
    qw(
       ABSOLUTE   ACTION   ADD   ALL
       ALLOCATE   ALTER   AND
       ANY   ARE
       AS   ASC
       ASSERTION   AT
       AUTHORIZATION   AVG
       BEGIN   BETWEEN   BIT   BIT_LENGTH
       BOTH   BY
       CASCADE   CASCADED   CASE   CAST
       CATALOG
       CHAR   CHARACTER   CHAR_LENGTH
       CHARACTER_LENGTH   CHECK   CLOSE   COALESCE
       COLLATE   COLLATION
       COLUMN   COMMIT
       CONNECT
       CONNECTION   CONSTRAINT
       CONSTRAINTS   CONTINUE
       CONVERT   CORRESPONDING   COUNT   CREATE   CROSS
       CURRENT
       CURRENT_DATE   CURRENT_TIME
       CURRENT_TIMESTAMP   CURRENT_USER   CURSOR
       DATE   DAY   DEALLOCATE   DEC
       DECIMAL   DECLARE   DEFAULT   DEFERRABLE
       DEFERRED   DELETE   DESC   DESCRIBE   DESCRIPTOR
       DIAGNOSTICS
       DISCONNECT   DISTINCT   DOMAIN   DOUBLE   DROP
       ELSE   END   END_EXEC   ESCAPE
       EXCEPT   EXCEPTION
       EXEC   EXECUTE   EXISTS
       EXTERNAL   EXTRACT
       FALSE   FETCH   FIRST   FLOAT   FOR
       FOREIGN   FOUND   FROM   FULL
       GET   GLOBAL   GO   GOTO
       GRANT   GROUP
       HAVING   HOUR
       IDENTITY   IMMEDIATE   IN   INDICATOR
       INITIALLY   INNER   INPUT
       INSENSITIVE   INSERT   INT   INTEGER   INTERSECT
       INTERVAL   INTO   IS
       ISOLATION
       JOIN
       KEY
       LANGUAGE   LAST   LEADING   LEFT
       LEVEL   LIKE   LOCAL   LOWER
       MATCH   MAX   MIN   MINUTE   MODULE
       MONTH
       NAMES   NATIONAL   NATURAL   NCHAR   NEXT   NO
       NOT   NULL
       NULLIF   NUMERIC
       OCTET_LENGTH   OF
       ON   ONLY   OPEN   OPTION   OR
       ORDER   OUTER
       OUTPUT   OVERLAPS
       PAD   PARTIAL   POSITION   PRECISION   PREPARE
       PRESERVE   PRIMARY
       PRIOR   PRIVILEGES   PROCEDURE   PUBLIC
       READ   REAL   REFERENCES   RELATIVE   RESTRICT
       REVOKE   RIGHT
       ROLLBACK   ROWS
       SCHEMA   SCROLL   SECOND   SECTION
       SELECT
       SESSION   SESSION_USER   SET
       SIZE   SMALLINT   SOME   SPACE   SQL   SQLCODE
       SQLERROR   SQLSTATE
       SUBSTRING   SUM   SYSTEM_USER
       TABLE   TEMPORARY
       THEN   TIME   TIMESTAMP
       TIMEZONE_HOUR   TIMEZONE_MINUTE
       TO   TRAILING   TRANSACTION
       TRANSLATE   TRANSLATION   TRIM   TRUE
       UNION   UNIQUE   UNKNOWN   UPDATE   UPPER   USAGE
       USER   USING
       VALUE   VALUES   VARCHAR   VARYING   VIEW
       WHEN   WHENEVER   WHERE   WITH   WORK   WRITE
       YEAR
       ZONE);

my  @non_reserved_word = 
    qw(
       ADA
       C   CATALOG_NAME
       CHARACTER_SET_CATALOG   CHARACTER_SET_NAME
       CHARACTER_SET_SCHEMA   CLASS_ORIGIN   COBOL   COLLATION_CATALOG
       COLLATION_NAME   COLLATION_SCHEMA   COLUMN_NAME   COMMAND_FUNCTION
       COMMITTED
       CONDITION_NUMBER   CONNECTION_NAME  CONSTRAINT_CATALOG  CONSTRAINT_NAME
       CONSTRAINT_SCHEMA   CURSOR_NAME
       DATA   DATETIME_INTERVAL_CODE
       DATETIME_INTERVAL_PRECISION   DYNAMIC_FUNCTION
       FORTRAN
       LENGTH
       MESSAGE_LENGTH   MESSAGE_OCTET_LENGTH   MESSAGE_TEXT   MORE   MUMPS
       NAME   NULLABLE   NUMBER
       PASCAL   PLI
       REPEATABLE   RETURNED_LENGTH   RETURNED_OCTET_LENGTH   RETURNED_SQLSTATE
       ROW_COUNT
       SCALE   SCHEMA_NAME   SERIALIZABLE   SERVER_NAME   SUBCLASS_ORIGIN
       TABLE_NAME   TYPE
       UNCOMMITTED   UNNAMED);

    my $r1 = join '|', @reserved_word ;
    my $r2 = join '|', @non_reserved_word;

    my $res_regex    = '(?i)^(' . $r1 . ')$';
    my $nonres_regex = '(?i)^(' . $r2 . ')$';

    %tokentype = 
        (
         LPAREN           => '^\($',                         # '
         RPAREN           => '^\)$',                         # '
         COMMA            => '^\,$',                         # '
         RESERVED_WORD    => $res_regex,
         NONRESERVED_WORD => $nonres_regex,
         WHITESPACE       => '^(\s+)$',                      # '
         NUMERIC_LITERAL  => '^-?(?:\d+(?:\.\d*)?|\.\d+)$',  # '
         DOUBLEQUOTESTR   => '^\"(.*)\"$',                   # '
         SINGLEQUOTESTR   => '^\'(.*)\'$'                    # '
         );

   %reloptype = 
        (
         SPACESHIP        => '<=>',                         # '
         
#         PERLREFOP        => '->', # ' fix perl code in sql statements

         DOUBLE_EQ        => '==',                         # '
         NOT_EQ1          => '!=',                         # '
         NOT_EQ2          => '<>',                         # '
         GT_EQ            => '>=',                         # '
         LT_EQ            => '<=',                         # '

         REGM            => '=~',                         # '
         NOT_REGM        => '!~',                         # '

         SINGLE_EQ        => '=',                         # '
         LESS_THAN        => '<',                         # '
         GREAT_THAN       => '>',                         # '
         );

   # group the relops by length - search longest match first. Would
   # greedy matching do this automatically?

   while (my ($tname, $tregexp) = each (%reloptype))
   {
       push @{$relopary[length($tregexp)]}, $tname;
   }

}

sub _init
{
    my $self = shift;

    $self->{pretty}   = '';
    $self->{badparse} = 0;
    $self->{decorate} = 0; # XXX XXX: maybe base on ANSIColor

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

sub Parseall
{
    my $self = shift;
    my $line = shift;

    $line =~ s/^\s+//;
    $line =~ s/\s+$//;

    $self->{prevpretty} = [];
    $self->{pretty}   = '';
    $self->{badparse} = 0;

    $self->{line} = $line;

    my @foo;
    push @foo, $line;
    my $prewords = $self->_lex(\@foo);
    my $cwords = $self->_classify($prewords);

    $self->{token_list} = $cwords;

    if (0)
    {
    greet $cwords;

    my @ggg;
    
    for (1..3)
    {
        push @ggg,  $self->Pop();
    }

    greet @ggg;
    greet $self;
}

}

sub Seek
{
#    whoami;
    my $self = shift;
    my ($badparse, $regexp, $getwhitespace, $peekahead) = @_;
    my ($token,$msg);

    $peekahead = 0
        unless (defined($peekahead));

    while (1)
    {

        # XXX XXX : always look at whitespace to avoid peek
        # discrepancy -- will peek at same token multiple times when
        # avoiding whitespace -- need to fix offsets
        ($token, $badparse, $msg) = 
            $self->Peek($badparse, $peekahead, 1);

#        greet $token, $badparse, $msg;

        last
            unless (defined($token));

        last
            if ($token->{val} =~ m/$regexp/);

        last 
            if ($badparse);

        $peekahead++;
    }
    return ($token, $badparse, $msg, $peekahead);

}

sub Peek
{
    my $self = shift;
    my ($badparse, $peekahead, $getwhitespace) = @_;
    my $msg;

    $badparse = 0
        unless (defined($badparse));
    $getwhitespace = 0
        unless (defined($getwhitespace));
    $peekahead = 0
        unless (defined($peekahead));

    my $maxcnt = scalar(@{$self->{token_list}});

    while ($peekahead < $maxcnt)
    {
        my $t1 = $self->{token_list}->[$peekahead];

        unless ($badparse)
        {
            if (exists($t1->{err}))
            {
                $msg = $t1->{err};
                $badparse = 1;
            }
        }

        $peekahead++;

        unless ($getwhitespace)
        {
            next
                if ((exists($t1->{type}))
                    && ('WHITESPACE' eq $t1->{type}));
        }
        return ($t1, $badparse, $msg);

    }
    return (undef, $badparse, $msg);
}

sub Pop
{
#    whoami;
    my $self = shift;
    my ($badparse, $getwhitespace) = @_;
    my $msg;

    $badparse = 0
        unless (defined($badparse));
    $getwhitespace = 0
        unless (defined($getwhitespace));

    if ($badparse)
    {
#        $self->{badparse} = length($self->{pretty});
        $self->{badparse} = 
            $self->{prevpretty}->[-1];
    }

    return $self->_innerPop($self->{token_list}, $badparse, $getwhitespace);
}

sub _innerPop
{
#    whoami;
    my $self = shift;
    my ($tlist, $badparse, $getwhitespace, $msg) = @_;

    while (scalar(@{$tlist}))
    {
        my $t1 = shift @{$tlist};

        unless ($badparse)
        {
            if (exists($t1->{err}))
            {
                $msg = $t1->{err};
                $self->{badparse} = length($self->{pretty});
                $badparse = 1;
            }
        }

        push @{$self->{prevpretty}}, length($self->{pretty});

        if ((exists($t1->{type}))
            && ('PAREN_LIST' eq $t1->{type}))
        {
            pop @{$self->{prevpretty}};
#            greet $t1->{val};
            # XXX XXX : resplice the token list, flattening in the paren list
            unshift @{$tlist}, @{$t1->{val}};
            return $self->_innerPop($tlist, $badparse, $getwhitespace, $msg);
        }
        else
        {
            my $piece = $t1->{val};

            if ($self->{decorate})
            {
                # XXX XXX: decoration mode to set colors or underline
                # on reserved words. But need to fix badparse length
                # info to point at keyword correctly

#         $piece = Term::ANSIColor::colored($piece, "BOLD");
#         , "UNDERSCORE");
                $piece = uc ($piece)
                    if ($t1->{type} eq 'RESERVED_WORD');
            }

            $self->{pretty} .= $piece;
        }

        next
            if $badparse;

        unless ($getwhitespace)
        {
            next
                if ((exists($t1->{type}))
                    && ('WHITESPACE' eq $t1->{type}));
        }
        return ($t1, $badparse, $msg);
    }
    return (undef, $badparse, $msg);

}

# take a ref to an array of strings and subdivide further, preserving
# quoted strings, and separating based upon whitespace and unquoted
# comma's and parentheses.  Search for a specified terminator token in
# the original stream, and honor parenthetical expressions -- the
# terminator must be outside of a right paren.  Leave the terminator
# token in the original word list.
sub _lex
{
#    whoami;
    my $self = shift;
    my $allwords = shift;

    my $paren_depth = 0;

    my @toklist;

    while (scalar(@{$allwords}))
    {
        my $line = shift @{$allwords};
        next
            unless (defined($line) && length($line));

        # keep the delimiters, ie commas, parens, spaces
        foreach my $t1 (parse_line('[,()\s]', 'delimiters', $line))
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

    return \@toklist; #, $paren_depth;

}

sub _classify
{
#    whoami;
    my $self = shift;
    my $allwords = shift;

    my $paren_depth = 0;

    my @liststack;
    my $token_list = [];

    my @aw2;

    push @aw2, @{$allwords};

  L_while1:
    while (scalar(@aw2))
    {
        my $t1 = shift @aw2;

        next
            unless ((defined ($t1)) && length($t1));

        my $tokdesc;

        $tokdesc = {};

        {
            if ($t1 =~ m/^\($/ ) # left paren
            {
                $paren_depth++;
                
                $tokdesc->{type} = 'LPAREN';
                
                push @liststack, $token_list;
                $token_list = [];
            }
            
            unless (exists($tokdesc->{type}))
            {
                # XXX XXX : reset firstkey for the hash
                my $a = scalar keys %tokentype;
                
              L_eachregexp:
                while (my ($tname, $tregexp) = each (%tokentype))
                {
                    if ($t1 =~ m/$tregexp/)
                    {
                        $tokdesc->{type} = $tname;
                        last  L_eachregexp;
                    }
                }
            }
            
            unless (exists($tokdesc->{type}))
            {
                if (($t1 !~ m/^\d/) &&    # doesn't start with number
                    ($t1 =~ m/^\w+$/))    # number, underscore, alpha
                {
                    # Note: not as strict as sql2
                    $tokdesc->{type} = 'IDENTIFIER';
                }
                
            }

            unless (exists($tokdesc->{type}))
            {
                $tokdesc->{type} = 'unknown';

                my $last_one = scalar(@relopary);

              L_relop:
                for my $ii (0..$last_one)
                {
                    my $ary = $relopary[$last_one - $ii];

                    if (defined($ary) && scalar(@{$ary}))
                    {
                        for my $relop (@{$ary})
                        {
                            my $regexp = $reloptype{$relop};

                            if ($t1 =~ m/$regexp/)
                            {
                                if ($t1 =~ m/^$regexp$/)
                                {
                                    $tokdesc->{type} = $relop;
                                    last L_relop;
                                }

                                # split out the relop and push back
                                # into the token stream
                                
                                my @ggg = split(/$regexp/, $t1, 2);

                              L_splitty:
                                while (scalar(@ggg))
                                {
                                    my $t2 = pop @ggg;
                                    if (defined($t2) && length($t2))
                                    {
                                        unshift @aw2, $t2;
                                    }
                                    if (defined($regexp))
                                    {
                                        unshift @aw2, $regexp;
                                        $regexp = ();
                                    }
                                } # end while

                                # RETRY the tokens
                                next L_while1;

                            }
                        } # end for
                    }
                } # end for relop
            }
            
            $tokdesc->{val} = $t1;
            
            push @{$token_list}, $tokdesc;
            
            if ((exists($tokdesc->{type}))
                && ($tokdesc->{type} eq 'RPAREN'))
            {
                $paren_depth--;
                
                if ($paren_depth >= 0)
                {
                    my $parenlist = $token_list;
                    $token_list = pop @liststack;
                    
                    $tokdesc = {};
                    $tokdesc->{type} = 'PAREN_LIST';
                    $tokdesc->{val} = $parenlist;
                    
                    push @{$token_list}, $tokdesc;
                }
                else
                {
                    $tokdesc->{err} = 'unmatched rparen';
                    warn "unmatched rparen";
                }
            }
        }
    }  # end while 1

    if (scalar(@liststack))
    {
        warn "No closing paren";

        for my $ll (reverse (@liststack))
        {
            my $tokdesc;

            $tokdesc = {};

            my $parenlist = $token_list;
            $tokdesc->{type} = 'PAREN_LIST';
            $tokdesc->{val} = $parenlist;
            $tokdesc->{err} = 'No closing parenthesis';            

            $token_list = $ll;
            push @{$token_list}, $tokdesc;
        }
    }

    return $token_list; #, $paren_depth;
}

END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Parse::FeebLex - Feeble Lexer

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 ARGUMENTS

=head1 FUNCTIONS

=head2 EXPORT

=head1 LIMITATIONS

various

=head1 TODO

=over 4

=item  quoted string support imperfect - case of 
       WHERE col1="if ($foo->{baz}) then blah();"
       not quite correct...

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
