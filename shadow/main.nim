import stew/endians2, stew/byteutils, tables, strutils, os
import vendor/nim-libp2p/libp2p, vendor/nim-libp2p/libp2p/protocols/pubsub/rpc/messages
import vendor/nim-libp2p/libp2p/muxers/mplex/lpchannel, vendor/nim-libp2p/libp2p/protocols/ping
import chronos
import sequtils, hashes, math, metrics
from times import getTime, toUnix, fromUnix, `-`, initTime, `$`, inMilliseconds, Duration
from nativesockets import getHostname

proc msgIdProvider(m: Message): Result[MessageId, ValidationResult] =
  return ok(($m.data.hash).toBytes())

proc main {.async.} =
  const
    numRows = 16
    numCols = 16
    custodyRows = 1
    custodyCols = 1
    blocksize = 2^19
    sendRows = true
    sendCols = true
    printGossipSubStats = false
  const
    interest = numRows * custodyCols + (numCols-custodyCols) * custodyRows
  let
    hostname = getHostname()
    myId = parseInt(hostname[4..^1])
    #publisherCount = client.param(int, "publisher_count")
    publisherCount = 10
    isPublisher = myId <= publisherCount
    #isAttacker = (not isPublisher) and myId - publisherCount <= client.param(int, "attacker_count")
    isAttacker = false
    rng = libp2p.newRng()
    #randCountry = rng.rand(distribCumSummed[^1])
    #country = distribCumSummed.find(distribCumSummed.filterIt(it >= randCountry)[0])
  let
    address = initTAddress("0.0.0.0:5000")
    switch =
      SwitchBuilder
        .new()
        .withAddress(MultiAddress.init(address).tryGet())
        .withRng(rng)
        #.withYamux()
        .withMplex()
        .withMaxConnections(10000)
        .withTcpTransport(flags = {ServerFlags.TcpNoDelay})
        #.withPlainText()
        .withNoise()
        .build()
    gossipSub = GossipSub.init(
      switch = switch,
#      triggerSelf = true,
      msgIdProvider = msgIdProvider,
      verifySignature = false,
      anonymize = true,
      )
    pingProtocol = Ping.new(rng=rng)
  gossipSub.parameters.floodPublish = false
  #gossipSub.parameters.lazyPushThreshold = 1_000_000_000
  #gossipSub.parameters.lazyPushThreshold = 0
  gossipSub.parameters.opportunisticGraftThreshold = -10000
  gossipSub.parameters.heartbeatInterval = 700.milliseconds
  gossipSub.parameters.pruneBackoff = 3.seconds
  gossipSub.parameters.gossipFactor = 0.05
  gossipSub.parameters.d = 8
  gossipSub.parameters.dLow = 6
  gossipSub.parameters.dHigh = 12
  gossipSub.parameters.dScore = 6
  gossipSub.parameters.dOut = 6 div 2
  gossipSub.parameters.dLazy = 6
  gossipSub.topicParams["test"] = TopicParams(
    topicWeight: 1,
    firstMessageDeliveriesWeight: 1,
    firstMessageDeliveriesCap: 30,
    firstMessageDeliveriesDecay: 0.9
  )

  var rows = toSeq(0..<numRows)
  if not isPublisher:
    rng.shuffle(rows)
    rows = rows[0..<custodyRows]

  var cols = toSeq(0..<numCols)
  if not isPublisher:
    rng.shuffle(cols)
    cols = cols[0..<custodyCols]


  proc dasTopicR(row: int) : string =
    "R" & $row

  proc dasTopicC(col: int) : string =
    "C" & $col

  proc messageLatency(data: seq[byte]) : times.Duration =
    let
      sentMoment = nanoseconds(int64(uint64.fromBytesLE(data)))
      sentNanosecs = nanoseconds(sentMoment - seconds(sentMoment.seconds))
      sentDate = initTime(sentMoment.seconds, sentNanosecs)
    result = getTime() - sentDate

  var messagesChunks = initTable[uint64, CountTable[(byte, byte)]]()
  var messagesChunkCount = initCountTable[uint64]()
  proc messageHandler(topic: string, data: seq[byte]) {.async.} =
    let
      sentUint = uint64.fromBytesLE(data)
      row = data[10]
      col = data[12]
    # warm-up
    if sentUint < 1000000: return
    #if isAttacker: return

    if not messagesChunks.hasKey(sentUint):
      messagesChunks[sentUint] = initCountTable[(byte, byte)]()

    messagesChunks[sentUint].inc((row,col))
    if messagesChunks[sentUint][(row,col)] > 1:
      echo sentUint, " DUP ms: ", messageLatency(data).inMilliseconds(), " row: ", row, " column: ", col
      return
    else:
      messagesChunkCount.inc(sentUint)
      echo "arrived: ", messagesChunkCount[sentUint], " of ", interest
      echo sentUint, " ARR ms: ", messageLatency(data).inMilliseconds(), " row: ", row, " column: ", col

    if messagesChunkCount[sentUint] < interest: return

    echo sentUint, " BLK ms: ", messageLatency(data).inMilliseconds(), " block arrived"
    echo sentUint, " milliseconds: ", messageLatency(data).inMilliseconds()

  var
    startOfTest: Moment
    attackAfter = 10000.hours
  proc messageValidator(topic: string, msg: Message): Future[ValidationResult] {.async.} =
    if isAttacker and Moment.now - startOfTest >= attackAfter:
      return ValidationResult.Ignore

    return ValidationResult.Accept

  for row in rows:
    gossipSub.subscribe(dasTopicR(row), messageHandler)
    gossipSub.addValidator([dasTopicR(row)], messageValidator)

  for col in cols:
    gossipSub.subscribe(dasTopicC(col), messageHandler)
    gossipSub.addValidator([dasTopicC(col)], messageValidator)

  switch.mount(gossipSub)
  switch.mount(pingProtocol)
  await switch.start()
  #TODO
  #defer: await switch.stop()

  echo "Listening on ", switch.peerInfo.addrs
  echo myId, ", ", isPublisher, ", ", switch.peerInfo.peerId

  var peersInfo = toSeq(1..parseInt(getEnv("PEERS")))
  rng.shuffle(peersInfo)

  proc pinger(peerId: PeerId) {.async.} =
    try:
      await sleepAsync(20.seconds)
      while true:
        let stream = await switch.dial(peerId, PingCodec)
        let delay = await pingProtocol.ping(stream)
        await stream.close()
        #echo delay
        await sleepAsync(delay)
    except:
      echo "Failed to ping"


  let connectTo = parseInt(getEnv("CONNECTTO"))
  var connected = 0
  for peerInfo in peersInfo:
    if connected >= connectTo: break
    let tAddress = "peer" & $peerInfo & ":5000"
    echo tAddress
    let addrs = resolveTAddress(tAddress).mapIt(MultiAddress.init(it).tryGet())
    try:
      let peerId = await switch.connect(addrs[0], allowUnknownPeerId=true).wait(5.seconds)
      #asyncSpawn pinger(peerId)
      connected.inc()
    except CatchableError as exc:
      echo "Failed to dial", exc.msg

  #let
  #  maxMessageDelay = client.param(int, "max_message_delay")
  #  warmupMessages = client.param(int, "warmup_messages")
  #startOfTest = Moment.now() + milliseconds(warmupMessages * maxMessageDelay div 2)

  await sleepAsync(10.seconds)
  # echo "Mesh size: ", gossipSub.mesh.getOrDefault("test").len
  for row in rows:
    let topic = dasTopicR(row)
    echo "Mesh size ", topic, " ", gossipSub.mesh.getOrDefault(topic).len
  for col in cols:
    let topic = dasTopicC(col)
    echo "Mesh size ", topic, " ", gossipSub.mesh.getOrDefault(topic).len

  for msg in 0 ..< 10:#client.param(int, "message_count"):
    await sleepAsync(12.seconds)
    if msg mod publisherCount == myId - 1:
    #if myId == 1:
      let
        now = getTime()
        nowInt = seconds(now.toUnix()) + nanoseconds(times.nanosecond(now))
      var nowBytes = @(toBytesLE(uint64(nowInt.nanoseconds))) & newSeq[byte](blocksize div (numRows*numCols))
      echo "sending ", uint64(nowInt.nanoseconds)

      for row in 0..<numRows:
        for col in 0..<numCols:
          nowBytes[10] = byte(row)
          nowBytes[12] = byte(col)
          echo "sending ", uint64(nowInt.nanoseconds), "r", row, "c", col
          if sendRows:
            discard gossipSub.publish(dasTopicR(row), nowBytes)
          if sendCols:
            discard gossipSub.publish(dasTopicC(col), nowBytes)

  #echo "BW: ", libp2p_protocols_bytes.value(labelValues=["/meshsub/1.1.0", "in"]) + libp2p_protocols_bytes.value(labelValues=["/meshsub/1.1.0", "out"])
  #echo "DUPS: ", libp2p_gossipsub_duplicate.value(), " / ", libp2p_gossipsub_received.value()

  when printGossipSubStats:
    #requires exporting counters from GossipSub.nim
    echo "statcounters: dup_during_validation ", libp2p_gossipsub_duplicate_during_validation.value(),
        "\tidontwant_saves ", libp2p_gossipsub_idontwant_saved_messages.value(),
        #"gossip optimization saves ", libp2p_gossipsub_saved_bytes.value(),
        "\tdup_received ", libp2p_gossipsub_duplicate.value(),
        "\tUnique_msg_received ", libp2p_gossipsub_received.value(),
        "\tStaggered_Saves ", libp2p_gossipsub_staggerSave.value(),
        "\tDontWant_IN_Stagger ", libp2p_gossipsub_staggerDontWantSave.value()
waitFor(main())
