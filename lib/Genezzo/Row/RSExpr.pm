#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Row/RCS/RSBlock.pm,v 6.3 2005/01/30 09:38:58 claude Exp claude $
#
# copyright (c) 2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
use strict;
use warnings;

package Genezzo::Row::RSExpr;

use Genezzo::Util;
use Genezzo::PushHash::PushHash;
use Carp;
use warnings::register;

our @ISA = "Genezzo::PushHash::PushHash" ;

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

    my $warn = 0;
    if (exists($args{severity}))
    {
        my $sev = uc($args{severity});
        $sev = 'WARNING'
            if ($sev =~ m/warn/i);

        # don't print 'INFO' prefix
        if ($args{severity} !~ m/info/i)
        {
            printf ("%s: ", $sev);
            $warn = 1;
        }

    }
    # XXX XXX XXX
    print __PACKAGE__, ": ",  $args{msg};
#    print $args{msg};
#    carp $args{msg}
#      if (warnings::enabled() && $warn);
    
};

sub _init
{
#    whoami;
    #greet @_;
    my $self      =  shift;

    my %required  =  (
                      rs => "no rowsource!"
                      );
    
    my %args = (@_);

    return 0
        unless (Validate(\%args, \%required));

    $self->{rs} = $args{rs};

    if (defined($args{select_list}))
    {
#        greet $args{select_list};
        $self->{select_list} = $args{select_list};
    }

    return 1;
}

sub TIEHASH
{ #sub new 
#    greet @_;
#    whoami;
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
#    my $self     = $class->SUPER::TIEHASH(@_);
    my $self     = {};

    my %args = (@_);
    return undef
        unless (_init($self,%args));

    if ((exists($args{GZERR}))
        && (defined($args{GZERR}))
        && (length($args{GZERR})))
    {
        # NOTE: don't supply our GZERR here - will get
        # recursive failure...
        $self->{GZERR} = $args{GZERR};
    }

    return bless $self, $class;
} # end new

sub SelectList
{
#    whoami;
    my $self = shift;

#    return undef; # XXX XXX XXX XXX XXX XXX 

    return $self->{select_list}
       if (exists($self->{select_list}));

    return undef;
}

# HPush public method (not part of standard hash)
sub HPush
{
    my $self = shift;
    my $rs = $self->{rs};

#    whoami;

    return ($rs->HPush(@_));
}

sub HCount
{
    my $self = shift;
    my $rs = $self->{rs};

    whoami;

    return ($rs->HCount(@_));
}

# standard hash methods follow
sub STORE
{
    my $self = shift;
    my $rs = $self->{rs};

    whoami;

    return ($rs->STORE(@_));
}
 
sub FETCH 
{
    my $self = shift;
    my $rs = $self->{rs};

    whoami;

    return ($rs->FETCH(@_));
}
sub FIRSTKEY 
{
    my $self = shift;
    my $rs = $self->{rs};

#    whoami;

    return ($rs->FIRSTKEY(@_));
}
sub NEXTKEY  
{
    my $self = shift;
    my $rs = $self->{rs};

#    whoami;

    return ($rs->NEXTKEY(@_));
}
sub EXISTS   
{
    my $self = shift;
    my $rs = $self->{rs};

#    whoami;

    return ($rs->EXISTS(@_));
}
sub DELETE   
{
    my $self = shift;
    my $rs = $self->{rs};

#    whoami;

    return ($rs->DELETE(@_));
}
sub CLEAR    
{
    my $self = shift;
    my $rs = $self->{rs};

#    whoami;

    return ($rs->CLEAR(@_));
}

sub AUTOLOAD 
{
    my $self = shift;
    my $rs = $self->{rs};

    our $AUTOLOAD;
    my $newfunc = $AUTOLOAD;
    $newfunc =~ s/.*:://;
    return if $newfunc eq 'DESTROY';

#    greet $newfunc;
    return ($rs->$newfunc(@_));
}

sub SQLPrepare # get a DBI-style statement handle
{
    my $self = shift;
    my %args = @_;
    $args{pushhash} = $self;
    $args{rs}       = $self->{rs};
    if (defined($self->{select_list}))
    {
        $args{select_list} = $self->{select_list};
    }
    $args{use_select_list} = defined($self->SelectList());

    if ((exists($self->{GZERR}))
        && (defined($self->{GZERR})))
    {
        $args{GZERR} = $self->{GZERR};
    }

    my $sth = Genezzo::Row::SQL_RSExpr->new(%args);

    return $sth;
}

package Genezzo::Row::SQL_RSExpr;
use strict;
use warnings;
use Genezzo::Util;

sub _init
{
    my $self = shift;
    my %args = (@_);

    return 0
        unless (defined($args{pushhash}));
    $self->{pushhash} = $args{pushhash};

    return 0
        unless (defined($args{rs}));
    my $rs = $args{rs};
    $self->{sql_rs}   = $rs->SQLPrepare(@_);
    return 0
        unless (defined($self->{sql_rs}));

    if (defined($args{select_list}))
    {
#        greet $args{select_list};
        $self->{select_list} = $args{select_list};
    }

    $self->{rownum} = 0;
    $self->{use_select_list} = $args{use_select_list};

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
    }

    return undef
        unless (_init($self,%args));

    return bless $self, $class;

} # end new

sub SQLFetch
{
    my $self = shift;
    my $rs = $self->{sql_rs};

#    whoami;

    my $tc_rownum = $self->{rownum} + 1;

#    my ($tc_rid, $vv) = $rs->SQLFetch(@_);
    my ($rid, $vv) = $rs->SQLFetch(@_);
    greet $rid, $vv;

    my @big_arr;

    if (defined($vv))
    {
        if ($self->{use_select_list})
        {
            my $outarr = $vv;

            for my $valex (@{$self->{select_list}})
            {
                unless (defined($valex->{value_expression}))
                {
                    my $msg = "no value expression!";
                    my %earg = (self => $self, msg => $msg,
                                severity => 'warn');
        
                    &$GZERR(%earg)
                        if (defined($GZERR));
                    return undef;
                }
                unless (defined($valex->{value_expression}->{vx}))
                {
                    my $msg = "no value expression vx!";
                    my %earg = (self => $self, msg => $msg,
                                severity => 'warn');
        
                    &$GZERR(%earg)
                        if (defined($GZERR));
                    return undef;
                }
                
                my $vx_val;
                my $v_str = 
                    '$vx_val = ' . $valex->{value_expression}->{vx} . ';' ;

#                whoami $v_str;

                {
                    my $msg = "";
                    my $status = eval "$v_str";

                    unless (defined($status))
                    {
                        # $@ must be non-null if eval failed
                        $msg .= $@ 
                            if $@;
                    }

                    # NOTE: status of undef is ok if no warning message
                    if (defined($status) || !(length($msg)))
                    {
                        push @big_arr, $vx_val;
                    }
                    else
                    {
#        warn $@ if $@;
                        $msg .= "\nbad value expression:\n";
                        $msg .= $valex->{value_expression}->{vx} . "\n";

                        my %earg = (self => $self, msg => $msg,
                                severity => 'warn');
                        
                        &$GZERR(%earg)
                            if (defined($GZERR));
                        
                        greet $outarr;

                        return undef;
                    }
                }
            } # end for all valex

        }
        else
        {
            push @big_arr, @{$vv};
        }
        $self->{rownum} += 1;
    }

#    return ($tc_rid, \@big_arr);
    return ($rid, \@big_arr);
}

sub AUTOLOAD 
{
    my $self = shift;
    my $rs = $self->{sql_rs};

    our $AUTOLOAD;
    my $newfunc = $AUTOLOAD;
    $newfunc =~ s/.*:://;
    return if $newfunc eq 'DESTROY';

#    greet $newfunc;
    return ($rs->$newfunc(@_));
}


END {

}

1;

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Row::RSBlock - Row Source Block

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

=cut

