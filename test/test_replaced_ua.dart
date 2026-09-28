import 'dart:async';
import 'dart:io';

import 'package:test/test.dart';

import 'package:sip_ua/sip_ua.dart';
import 'package:sip_ua/src/event_manager/event_manager.dart';
import 'package:sip_ua/src/rtc_session.dart';
import 'package:sip_ua/src/ua.dart';

/// Accepts the socket and records REGISTER, but never answers it, so the
/// client transaction is still alive when start() is called again.
Future<HttpServer> _hangingRegistrar(Completer<void> sawRegister) async {
  HttpServer server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  server.listen((HttpRequest req) {
    unawaited(() async {
      WebSocket socket = await WebSocketTransformer.upgrade(req);
      socket.listen((dynamic msg) {
        if (msg.toString().startsWith('REGISTER ') &&
            !sawRegister.isCompleted) {
          sawRegister.complete();
        }
      });
    }());
  });
  return server;
}

/// Hands the test the server side of the first connection, so that it can
/// send the UA a request.
Future<HttpServer> _peer(Completer<WebSocket> first) async {
  HttpServer server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  server.listen((HttpRequest req) {
    unawaited(() async {
      WebSocket socket = await WebSocketTransformer.upgrade(req);
      if (!first.isCompleted) {
        first.complete(socket);
      }
      socket.listen((dynamic _) {});
    }());
  });
  return server;
}

const String _invite = 'INVITE sip:alice@127.0.0.1 SIP/2.0\r\n'
    'Via: SIP/2.0/WS 127.0.0.1;branch=z9hG4bKreplaced1\r\n'
    'Max-Forwards: 70\r\n'
    'From: <sip:bob@127.0.0.1>;tag=bob1\r\n'
    'To: <sip:alice@127.0.0.1>\r\n'
    'Call-ID: replaced-ua-call\r\n'
    'CSeq: 1 INVITE\r\n'
    'Contact: <sip:bob@127.0.0.1;transport=ws>\r\n'
    // Without it an INVITE is refused with 415. The SDP is read only on
    // answer, which this test never does.
    'Content-Type: application/sdp\r\n'
    'Content-Length: 0\r\n'
    '\r\n';

List<void Function()> testFunctions = <void Function()>[
  () => test('replaced UA does not emit transport or registration events',
          () async {
        Completer<void> sawRegister = Completer<void>();
        HttpServer server = await _hangingRegistrar(sawRegister);
        SIPUAHelper helper = SIPUAHelper();
        _Recorder recorder = _Recorder();
        helper.addSipUaHelperListener(recorder);
        addTearDown(() async {
          helper.stop();
          await Future<void>.delayed(const Duration(milliseconds: 2500));
          await server.close(force: true);
        });

        UaSettings settings = UaSettings();
        settings.transportType = TransportType.WS;
        settings.webSocketUrl = 'ws://127.0.0.1:${server.port}/sip';
        settings.uri = 'sip:alice@127.0.0.1';
        settings.authorizationUser = 'alice';
        settings.password = 'secret';
        settings.register = true;

        await helper.start(settings);
        await recorder.connected.future.timeout(const Duration(seconds: 5));
        await sawRegister.future.timeout(const Duration(seconds: 5));

        recorder.clear();
        await helper.start(settings);
        await recorder.connected.future.timeout(const Duration(seconds: 5));
        // stop() waits 2s before disconnecting a UA that still has a transaction.
        // Timer F is 32s, so a failure inside this window is that deferred close.
        await Future<void>.delayed(const Duration(milliseconds: 2500));

        expect(
            recorder.transports.where((TransportState state) =>
                state.state == TransportStateEnum.DISCONNECTED &&
                state.cause?.reason_phrase == 'close by local'),
            isEmpty);
        expect(
            recorder.registrations.where((RegistrationState state) =>
                state.state == RegistrationStateEnum.REGISTRATION_FAILED),
            isEmpty);
      }),
  () =>
      test('call events of a replaced UA still reach the listeners', () async {
        Completer<WebSocket> peer = Completer<WebSocket>();
        HttpServer server = await _peer(peer);
        SIPUAHelper helper = SIPUAHelper();
        _Recorder recorder = _Recorder();
        helper.addSipUaHelperListener(recorder);
        addTearDown(() async {
          helper.stop();
          await Future<void>.delayed(const Duration(milliseconds: 2500));
          await server.close(force: true);
        });

        UaSettings settings = UaSettings();
        settings.transportType = TransportType.WS;
        settings.webSocketUrl = 'ws://127.0.0.1:${server.port}/sip';
        settings.uri = 'sip:alice@127.0.0.1';
        settings.register = false;

        await helper.start(settings);
        await recorder.connected.future.timeout(const Duration(seconds: 5));
        // The UA is not public; an incoming call is the way to reach it.
        (await peer.future).add(_invite);
        Call incoming =
            await recorder.firstCall.future.timeout(const Duration(seconds: 5));
        UA oldUa = incoming.session.ua;

        // Stands in for a confirmed call of the old UA. stop() runs its
        // terminate(), which emits ENDED only after `await _logCallStat()`:
        // by then start() has replaced the UA. A unit test cannot confirm a
        // real call without WebRTC, so the session is built here and ENDED
        // is emitted by hand. It is not in the old UA's session list, so
        // stop() does not end it first.
        RTCSession confirmed = RTCSession(oldUa);
        confirmed.addAllEventHandlers(
            helper.buildCallOptions()['eventHandlers'] as EventManager);
        oldUa.emit(EventNewRTCSession(
            session: confirmed, originator: Originator.local));

        recorder.clear();
        await helper.start(settings);
        await recorder.connected.future.timeout(const Duration(seconds: 5));
        confirmed.emit(EventCallEnded(
            session: confirmed,
            originator: Originator.local,
            cause: ErrorCause(
                cause: 'Terminated', status_code: 200, reason_phrase: 'BYE')));

        expect(
            recorder.calls.where((_CallChange change) =>
                identical(change.call.session, confirmed) &&
                change.state.state == CallStateEnum.ENDED),
            hasLength(1));
      }),
];

void main() {
  for (Function func in testFunctions) {
    func();
  }
}

class _Recorder implements SipUaHelperListener {
  Completer<void> connected = Completer<void>();
  final Completer<Call> firstCall = Completer<Call>();
  final List<TransportState> transports = <TransportState>[];
  final List<RegistrationState> registrations = <RegistrationState>[];
  final List<_CallChange> calls = <_CallChange>[];

  void clear() {
    transports.clear();
    registrations.clear();
    calls.clear();
    connected = Completer<void>();
  }

  @override
  void transportStateChanged(TransportState state) {
    transports.add(state);
    if (state.state == TransportStateEnum.CONNECTED && !connected.isCompleted) {
      connected.complete();
    }
  }

  @override
  void registrationStateChanged(RegistrationState state) {
    registrations.add(state);
  }

  @override
  void callStateChanged(Call call, CallState state) {
    calls.add(_CallChange(call, state));
    if (!firstCall.isCompleted) {
      firstCall.complete(call);
    }
  }

  @override
  void onNewMessage(SIPMessageRequest msg) {}

  @override
  void onNewNotify(Notify ntf) {}

  @override
  void onNewReinvite(ReInvite event) {}

  @override
  void onNewInfo(SipInfo info) {}
}

class _CallChange {
  _CallChange(this.call, this.state);

  final Call call;
  final CallState state;
}
