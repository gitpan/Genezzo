#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Havok/RCS/SQLScalar.pm,v 1.4 2006/05/07 06:43:55 claude Exp claude $
#
# copyright (c) 2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Havok::SQLScalar;
require Exporter;

@ISA = qw(Exporter);
@EXPORT = qw(&sql_func_chomp 
             &sql_func_chop
             &sql_func_chr
             &sql_func_crypt
             &sql_func_index
             &sql_func_lc
             &sql_func_lcfirst
             &sql_func_length
             &sql_func_ord
             &sql_func_pack
             &sql_func_reverse
             &sql_func_rindex
             &sql_func_sprintf
             &sql_func_substr
             &sql_func_uc
             &sql_func_ucfirst
             &sql_func_abs
             &sql_func_atan2
             &sql_func_cos
             &sql_func_exp
             &sql_func_hex
             &sql_func_int
             &sql_func_log10
             &sql_func_oct
             &sql_func_rand
             &sql_func_sin
             &sql_func_sqrt
             &sql_func_srand
             &sql_func_perl_join
             
             &sql_func_concat
             &sql_func_greatest
             &sql_func_initcap
             &sql_func_least
             &sql_func_lower
             &sql_func_upper

             &sql_func_cosh
             &sql_func_ceil
             &sql_func_floor
             &sql_func_ln
             &sql_func_sinh
             &sql_func_tan
             &sql_func_tanh

             &sql_func_ascii
             &sql_func_instr        
             &sql_func_nvl         
             );

use Genezzo::Util;

use strict;
use warnings;

use Carp;

our $VERSION;
our $MAKEDEPS;

BEGIN {
    $VERSION = do { my @r = (q$Revision: 1.4 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

    my $pak1  = __PACKAGE__;
    $MAKEDEPS = {
        'NAME'     => $pak1,
        'ABSTRACT' => ' ',
        'AUTHOR'   => 'Jeffrey I Cohen (jcohen@cpan.org)',
        'LICENSE'  => 'gpl',
        'VERSION'  =>  $VERSION,
        }; # end makedeps

    $MAKEDEPS->{'PREREQ_HAVOK'} = {
        'Genezzo::Havok::UserFunctions' => '0.0',
    };

    # DML is an array, not a hash

    my $now = 
    do { my @r = (q$Date: 2006/05/07 06:43:55 $ =~ m|Date:(\s+)(\d+)/(\d+)/(\d+)(\s+)(\d+):(\d+):(\d+)|); sprintf ("%04d-%02d-%02dT%02d:%02d:%02d", $r[1],$r[2],$r[3],$r[5],$r[6],$r[7]); };


    my %tabdefs = ();
    $MAKEDEPS->{'TABLEDEFS'} = \%tabdefs;

    my @perl_funcs = qw(
                        chomp
                        chop
                        chr
                        crypt
                        index
                        lc
                        lcfirst
                        length
                        ord
                        pack
                        reverse
                        rindex
                        sprintf
                        substr
                        uc
                        ucfirst
                        abs
                        atan2
                        cos
                        exp
                        hex
                        int
                        log10
                        oct
                        rand
                        sin
                        sqrt
                        srand

                        perl_join
                        );

    my @sql_funcs = qw(
                       concat
                       greatest
                       initcap
                       least
                       lower
                       upper

                       cosh
                       ceil
                       floor
                       ln
                       sinh
                       tan
                       tanh
                       
                       ascii
                       instr
                       nvl
                       );
    my @ins1;
    my $ccnt = 1;
    for my $pfunc (@perl_funcs)
    {
        my $bigstr = "i user_functions $ccnt require $pak1 " 
            . "sql_func_" . $pfunc . " SYSTEM $now 0 ";
        push @ins1, $bigstr;
        $ccnt++;
    }
    for my $pfunc (@sql_funcs)
    {
        my $bigstr = "i user_functions $ccnt require $pak1 " 
            . "sql_func_" . $pfunc . " SYSTEM $now 0";

        if ($pfunc =~ m/^(greatest|least)$/i)
        {
            $bigstr .= " HASH";
        }

        push @ins1, $bigstr;
        $ccnt++;
    }

    # if check returns 0 rows then proceed with install
    $MAKEDEPS->{'DML'} = [
                          { check => [
                                      "select * from user_functions where xname = \'$pak1\'"
                                      ],
                            install => \@ins1
                            }
                          ];

#    print Data::Dumper->Dump([$MAKEDEPS]);
}

sub MakeYML
{
    use Genezzo::Havok;

    my $makedp = $MAKEDEPS;

    return Genezzo::Havok::MakeYML($makedp);
}

# perl scalar functions
# CHAR
sub sql_func_chomp
{
    # can't have full chomp semantics in sql...
    my $foo = shift;
    chomp($foo);
    return $foo;
}
sub sql_func_chop
{
    # can't have full chop semantics in sql...
    my $foo = shift;
    chop($foo);
    return $foo;
}
sub sql_func_chr
{
    my $num = shift;
    return chr($num);
}
sub sql_func_crypt
{
    return crypt $_[0], $_[1];
}
sub sql_func_index
{
    my $str = shift;
    my $substr = shift;
    my $pos = shift;
    $pos = 0 unless (defined($pos));
    return index $str, $substr, $pos;
}
sub sql_func_lc
{
    my $str = shift;
    return lc($str);
}
sub sql_func_lcfirst
{
    my $str = shift;
    return lcfirst($str);
}
sub sql_func_length
{
    my $str = shift;
    return length($str);
}
sub sql_func_ord
{
    my $str = shift;
    return ord($str);
}
sub sql_func_pack
{
return pack(@_);
}
sub sql_func_reverse
{
return reverse(@_);
}
sub sql_func_rindex
{
    my $str = shift;
    my $substr = shift;
    my $pos = shift;
    $pos = 0 unless (defined($pos));
    return rindex $str, $substr, $pos;
}
sub sql_func_sprintf
{
return sprintf(@_);
}
sub sql_func_substr
{
    my $exp1 = shift;
    my $off1 = shift;
    return substr $exp1, $off1, @_;
}
sub sql_func_uc
{
    my $str = shift;
    return uc($str);
}
sub sql_func_ucfirst
{
    my $str = shift;
    return ucfirst($str);
}

# perl scalar functions
# NUM
sub sql_func_abs
{
    my $num = shift;
    return abs($num);
}
sub sql_func_atan2
{
    my $yval = shift;
    my $xval = shift;
    return atan2 $yval, $xval;
}
sub sql_func_cos
{
    my $num = shift;
    return cos($num);
}
sub sql_func_exp
{
    my $num = shift;
    return exp($num);
}
sub sql_func_hex
{
    my $num = shift;
    return hex($num);
}
# XXX XXX: bad name?
sub sql_func_int
{
    my $num = shift;
    return int($num);
}

# Note: need to disambiguate, because perl "log" is natural log, 
#       but sql "log" is log10
sub sql_func_log10
{
    my $n = shift;
    return log($n)/log(10);
}
sub sql_func_oct
{
    my $num = shift;
    return oct($num);
}
sub sql_func_rand
{
    my $num = shift;
    return rand($num);
}
sub sql_func_sin
{
    my $num = shift;
    return sin($num);
}
sub sql_func_sqrt
{
    my $num = shift;
    return sqrt($num);
}
sub sql_func_srand
{
    my $num = shift;
    return srand($num);
}

# more perl
sub sql_func_perl_join
{
    my $p1 = shift;
    return join($p1, @_);
}

# SQL scalar functions
# CHAR
sub sql_func_concat
{
    return join('',@_);
}

sub sql_func_greatest
{
    my $maxval = shift;

    for my $val (@_)
    {
        if ($val gt $maxval)
        {
            $maxval = $val;
        }
    }
    return $maxval;
}

sub sql_func_initcap
{
    my $str = shift;
    return ucfirst($str);
}

sub sql_func_least
{
    my $minval = shift;

    for my $val (@_)
    {
        if ($val lt $minval)
        {
            $minval = $val;
        }
    }
    return $minval;
}

sub sql_func_lower
{
    my $str = shift;
    return lc($str);
}

sub sql_func_upper
{
    my $str = shift;
    return uc($str);
}


# SQL scalar functions
# num
sub sql_func_ceil
{
    return POSIX::ceil(@_);
}

sub sql_func_cosh
{
    # from Math::Complex - hyperbolic cosine cosh(z) = (exp(z) + exp(-z))/2.
    my $num = shift;
    return ((exp($num) + exp((-1) * $num))/2);
}


sub sql_func_floor
{
    return POSIX::floor(@_);
}

sub sql_func_ln
{
    my $n = shift;
    return log($n);
}

sub sql_func_sinh
{
    # from Math::Complex - hyperbolic sine sinh(z) = (exp(z) - exp(-z))/2.
    my $num = shift;
    return ((exp($num) - exp((-1) * $num))/2);
}

sub sql_func_tan
{
    my $num = shift;
    return (sin($num)/cos($num));
}

sub sql_func_tanh
{
    # from Math::Complex - hyperbolic tangent tanh(z) = sinh(z) / cosh(z).
    my $num = shift;
    return (sql_func_sinh($num) / sql_func_cosh($num));
}



# SQL scalar functions
# CONVERSION
sub sql_func_ascii
{
    my $str = shift;
    return ord($str);
}
sub sql_func_instr
{
    # XXX XXX: need to handle occurrence!!
    my ($str, $substr, $pos, $occurrence) = @_;
    $pos = 0 unless (defined($pos));
    if ($pos >= 0)
    {
        # instr starts at 1, and index starts at zero
        $pos-- if ($pos);
        return (index $str, $substr, $pos) + 1;
    }
    else
    {
        # oof! weird semantics...
        $str = reverse($str);
        $substr = reverse($substr);
        # instr starts at 1, and index starts at zero
        $pos++;
        $pos *= -1;
        return (index $str, $substr, $pos) + 1;
    }

}
sub sql_func_nvl
{
    my $s1 = shift;
    my $s2 = shift;

    if (defined($s1))
    {
        return $s1;
    }
    return $s2;
}

END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Havok::SQLScalar - scalar SQL functions

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item  abs 

=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS

#abs
 ceil
#cos
 cosh
#exp
 floor
 ln
#log - log 10
mod
power
round
sign
#sin
 sinh
#sqrt
 tan
 tanh
trunc

#chr
#concat
 initcap
 lower
lpad
ltrim
replace
rpad
rtrim
soundex
#substr
translate
 upper

 ascii
 instr
#length
 nvl



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
