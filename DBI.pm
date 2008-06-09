=head1 NAME

AnyEvent::DBI - asynchronous DBI access

=head1 SYNOPSIS

   use AnyEvent::DBI;

=head1 DESCRIPTION

This module is an L<AnyEvent> user, you need to make sure that you use and
run a supported event loop.

This module implements asynchronous DBI access my forking or executing
separate "DBI-Server" processes and sending them requests.

It means that you can run DBI requests in parallel to other tasks.

The overhead for very simple statements ("select 0") is somewhere
around 120% to 200% (single/dual core CPU) compared to an explicit
prepare_cached/execute/fetchrow_arrayref/finish combination.

=cut

package AnyEvent::DBI;

use strict;
no warnings;

use Carp;
use Socket ();
use Scalar::Util ();
use Storable ();

use DBI ();

use AnyEvent ();
use AnyEvent::Util ();

our $VERSION = '1.0';

# this is the forked server code

our $DBH;

sub req_open {
   my (undef, $dbi, $user, $pass, %attr) = @{+shift};

   $DBH = DBI->connect ($dbi, $user, $pass, \%attr);

   [1]
}

sub req_exec {
   my (undef, $st, @args) = @{+shift};

   my $sth = $DBH->prepare_cached ($st, undef, 1);

   $sth->execute (@args)
      or die $sth->errstr;

   [$sth->fetchall_arrayref]
}

sub serve {
   my ($fh) = @_;

   no strict;

   eval {
      my $rbuf;

      while () {
         sysread $fh, $rbuf, 16384, length $rbuf
            or last;

         while () {
            my $len = unpack "L", $rbuf;

            # full request available?
            last unless $len && $len + 4 <= length $rbuf;

            my $req = Storable::thaw substr $rbuf, 4;
            substr $rbuf, 0, $len + 4, ""; # remove length + request

            my $wbuf = eval { pack "L/a*", Storable::freeze $req->[0]($req) };

            $wbuf = pack "L/a*", Storable::freeze [undef, "$@"]
               if $@;

            for (my $ofs = 0; $ofs < length $wbuf; ) {
               $ofs += (syswrite $fh, substr $wbuf, $ofs
                           or die "unable to write results");
            }
         }
      }
   };

   kill 9, $$; # no other way on the broken windows platform
}

=head2 METHODS

=over 4

=item $dbh = new AnyEvent::DBI $database, $user, $pass, [key => value]...

Returns a database handle for the given database. Each database handle
has an associated server process that executes statements in order. If
you want to run more than one statement in parallel, you need to create
additional database handles.

The advantage of this approach is that transactions work as state is
preserved.

Example:

   $dbh = new AnyEvent::DBI
             "DBI:mysql:test;mysql_read_default_file=/root/.my.cnf", "", "";

Additional key-value pairs can be used to adjust behaviour:

=over 4

=item on_error => $callback->($dbh, $filename, $line, $fatal)

When an error occurs, then this callback will be invoked. On entry, C<$@>
is set to the error message. C<$filename> and C<$line> is where the
original request was submitted.

If this callback returns and this was a fatal error (C<$fatal> is true)
then AnyEvent::DBI die's, otherwise it calls the original request callback
without any arguments.

If omitted, then C<die> will be called on any errors, fatal or not.

=back

=cut

# stupid Storable autoloading, total loss-loss situation
Storable::thaw Storable::freeze [];

sub new {
   my ($class, $dbi, $user, $pass, %arg) = @_;

   socketpair my $client, my $server, &Socket::AF_UNIX, &Socket::SOCK_STREAM, &Socket::PF_UNSPEC
      or croak "unable to create dbi communicaiton pipe: $!";

   my $self = bless \%arg, $class;

   $self->{fh} = $client;

   Scalar::Util::weaken (my $wself = $self);

   AnyEvent::Util::fh_nonblocking $client, 1;

   my $rbuf;
   my @caller = (caller)[1,2]; # the "default" caller

   $self->{rw} = AnyEvent->io (fh => $client, poll => "r", cb => sub {
      my $len = sysread $client, $rbuf, 65536, length $rbuf;

      if ($len > 0) {

         while () {
            my $len = unpack "L", $rbuf;

            # full request available?
            last unless $len && $len + 4 <= length $rbuf;

            my $res = Storable::thaw substr $rbuf, 4;
            substr $rbuf, 0, $len + 4, ""; # remove length + request

            my $req = shift @{ $wself->{queue} };

            if (defined $res->[0]) {
               $req->[0](@$res);
            } else {
               my $cb = shift @$req;
               $wself->_error ($res->[1], @$req);
               $cb->();
            }
         }

      } elsif (defined $len) {
         $wself->_error ("unexpected eof", @caller, 1);
      } else {
         $wself->_error ("read error: $!", @caller, 1);
      }
   });

   $self->{ww_cb} = sub {
      my $len = syswrite $client, $wself->{wbuf}
         or return delete $wself->{ww};

      substr $wself->{wbuf}, 0, $len, "";
   };

   my $pid = fork;

   if ($pid) {
      # parent
      close $server;

   } elsif (defined $pid) {
      # child
      close $client;
      @_ = $server;
      goto &serve;

   } else {
      croak "fork: $!";
   }

   $self->_req (sub { }, (caller)[1,2], 1, req_open => $dbi, $user, $pass);

   $self
}

sub _error {
   my ($self, $error, $filename, $line, $fatal) = @_;

   delete $self->{rw};
   delete $self->{ww};
   delete $self->{fh};

   $@ = $error;

   $self->{on_error}($self, $filename, $line, $fatal)
      if $self->{on_error};

   die "$error at $filename, line $line\n";
}

sub _req {
   my ($self, $cb, $filename, $line, $fatal) = splice @_, 0, 5, ();

   push @{ $self->{queue} }, [$cb, $filename, $line, $fatal];

   $self->{wbuf} .= pack "L/a*", Storable::freeze \@_;

   unless ($self->{ww}) {
      my $len = syswrite $self->{fh}, $self->{wbuf};
      substr $self->{wbuf}, 0, $len, "";

      # still any left? then install a write watcher
      $self->{ww} = AnyEvent->io (fh => $self->{fh}, poll => "w", cb => $self->{ww_cb})
         if length $self->{wbuf};
   }
}

=item $dbh->exec ("statement", @args, $cb->($rows, %extra))

Executes the given SQL statement with placeholders replaced by
C<@args>. The statement will be prepared and cached on the server side, so
using placeholders is compulsory.

The callback will be called with the result of C<fetchall_arrayref> as
first argument and possibly a hash reference with additional information.

If an error occurs and the C<on_error> callback returns, then no arguments
will be passed and C<$@> contains the error message.

=cut

sub exec {
   my $cb = pop;
   splice @_, 1, 0, $cb, (caller)[1,2], 0, "req_exec";

   goto &_req;
}

=back

=head1 SEE ALSO

L<AnyEvent>, L<DBI>.

=head1 AUTHOR

   Marc Lehmann <schmorp@schmorp.de>
   http://home.schmorp.de/

=cut

1

