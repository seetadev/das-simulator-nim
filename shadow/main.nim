import stew/endians2, stew/byteutils, tables, strutils, os
import vendor/nim-libp2p/libp2p, vendor/nim-libp2p/libp2p/protocols/pubsub/rpc/messages
import vendor/nim-libp2p/libp2p/muxers/mplex/lpchannel, vendor/nim-libp2p/libp2p/protocols/ping
import chronos
import random # need since rng leads to "Error: internal error: could not find env param for segmentItRandom"
import sequtils, hashes, math, metrics
from times import getTime, toUnix, fromUnix, `-`, initTime, `$`, inMilliseconds, Duration
from nativesockets import getHostname

proc msgIdProvider(m: Message): Result[MessageId, ValidationResult] =
  return ok(($m.data.hash).toBytes())

proc main {.async.} =
  const
    blocksize = 2^21  # size of DAS block, before EC, in bytes
    numRows = 64      # number of Rows after EC
    numRowsK = 32     # number of Rows before EC
    numCols = 128
    numColsK = 64
    custodyRows = 1   # rows to custody (=topics to sbscribe)
    custodyCols = 1
    sendRows = true   # whether the publisher send out on row topics
    sendCols = true
    crossForward = false   # whether to relay received segments in the other dimension (row->col, col->row)
    publisherMaxCopies = 1  # how many copies of each segment to send out (see shufflepeers as well)
    publisherShufflePeers = true # how to select peers to send to. false: always the same; true: randomize
    publisherSendInRandomOrder = false  # whether to radomize segment order when publishing
    publisherSendRowCount = numRows # numRows: send whole row; numRowsK: send only half row
    publisherSendColCount = numCols
    repairOnTheFly = true # whether to repar as soon as a whole K arrived (both row and column)
    repairForward = false # whether to forward repaired chunks on the same line
    repairCrossForward = true # wheher to forward repaired segments on the other dimension

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

  proc isTopicR(topic: string) : bool =
    topic[0] == 'R'

  proc messageLatency(data: seq[byte]) : times.Duration =
    let
      sentMoment = nanoseconds(int64(uint64.fromBytesLE(data)))
      sentNanosecs = nanoseconds(sentMoment - seconds(sentMoment.seconds))
      sentDate = initTime(sentMoment.seconds, sentNanosecs)
    result = getTime() - sentDate

  var messagesChunks = initTable[uint64, CountTable[(int, int)]]()
  var messagesChunkCount = initCountTable[uint64]()
  proc messageHandler(topic: string, data: seq[byte]) {.async.} =
    let
      sentUint = uint64.fromBytesLE(data)
      row = data[10].int # TODO: use 2 bytes
      col = data[12].int
      roc = topic.isTopicR # Row or Column

    # warm-up
    if sentUint < 1000000: return
    #if isAttacker: return

    if not messagesChunks.hasKey(sentUint):
      messagesChunks[sentUint] = initCountTable[(int, int)]()

    proc sendOnCol(col: int, data: seq[byte]) =
          var rocData = data
          rocData[14] = 1
          discard gossipSub.publish(dasTopicC(int(col)), rocData)

    proc sendOnRow(row: int, data: seq[byte]) =
          var rocData = data
          rocData[14] = 0
          discard gossipSub.publish(dasTopicR(int(row)), rocData)

    if crossForward:
      if roc:
        if int(col) in cols:
          echo "crossing to col: ", col
          sendOnCol(col, data)
      else:
        if int(row) in rows:
          echo "crossing to row: ", row
          sendOnRow(row, data)

    messagesChunks[sentUint].inc((row,col))
    if messagesChunks[sentUint][(row,col)] > 1:
      echo sentUint, " DUP ms: ", messageLatency(data).inMilliseconds(), " row: ", row, " column: ", col
      return
    else:
      messagesChunkCount.inc(sentUint)
      echo "arrived: ", messagesChunkCount[sentUint], " of ", interest
      echo sentUint, " ARR ms: ", messageLatency(data).inMilliseconds(), " row: ", row, " column: ", col

    proc hasInRow(row:int) : int =
      for i in 0 ..< numCols :
        if messagesChunks[sentUint][(row, i)] >= 1:
          result += 1

    proc hasInCol(col:int) : int =
      for i in 0 ..< numRows :
        if messagesChunks[sentUint][(i, col)] >= 1:
          result += 1

    if repairOnTheFly:
      if int(row) in rows:
        if hasInRow(row) >= numColsK:
          for i in 0 ..< numCols :
            if messagesChunks[sentUint][(row, i)] == 0:
              messagesChunks[sentUint][(row, i)] = 1
              messagesChunkCount.inc(sentUint)
              if repairCrossForward:
                if int(col) in cols:
                  sendOnCol(col, data)
              if repairForward:
                sendOnRow(row, data)

      if int(col) in cols:
        if hasInCol(col) >= numRowsK:
          for i in 0 ..< numRows :
            if messagesChunks[sentUint][(i, col)] == 0:
              messagesChunks[sentUint][(i, col)] = 1
              messagesChunkCount.inc(sentUint)
              if repairCrossForward:
                if int(row) in rows:
                  sendOnRow(row, data)
              if repairForward:
                sendOnCol(col, data)

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
      var nowBytes = @(toBytesLE(uint64(nowInt.nanoseconds))) & newSeq[byte](blocksize div (numRowsK*numColsK))
      echo "sending ", uint64(nowInt.nanoseconds)

      iterator segmentItRC() : (int, int) =
        for row in 0..<publisherSendRowCount:
          for col in 0..<publisherSendColCount:
            yield (row, col)

      iterator segmentIt() : (int, int) {.inline.} =
        type segmentIdx = tuple[row: int, col: int]
        var segments : seq[segmentIdx]
        for rc in segmentItRC():
            segments.add(rc)
        if publisherSendInRandomOrder:
          # rnd.shuffle(segments) # TODO: this leads to "Error: internal error: could not find env param"
          random.shuffle(segments)
        for rc in segments:
          yield rc

      for (row, col) in segmentIt():
          nowBytes[10] = byte(row)
          nowBytes[12] = byte(col)
          echo "sending ", uint64(nowInt.nanoseconds), "r", row, "c", col
          if sendRows:
            nowBytes[14] = 0
            discard gossipSub.publish(dasTopicR(row), nowBytes, publisherMaxCopies, publisherShufflePeers)
          if sendCols:
            nowBytes[14] = 1
            discard gossipSub.publish(dasTopicC(col), nowBytes, publisherMaxCopies, publisherShufflePeers)

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
