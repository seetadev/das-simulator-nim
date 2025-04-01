# Optimism RetroPGF 7: Simulating Gossipsub Performance Metrics for DAS Using Shadow and libp2p  

**Description:**  
This project aims to simulate and analyze the performance of **Gossipsub** in the context of **Data Availability Sampling (DAS)** for Optimism using the **Shadow Network Simulator** and **libp2p**. The goal is to study network behavior, optimize message propagation efficiency, and evaluate DAS techniques under realistic conditions.  

The simulator (in development) consists of:  
- **Shadow Network Simulator** – An open-source framework to run real application code in a controlled network simulation environment.   
- **Custom DAS application logic** – Implementing different sampling strategies to evaluate performance across network conditions.  

This project builds on existing research in DAS and is inspired by the **Codex team’s** Nim-based DAS simulator. Unlike its Nim counterpart, this Python-based simulator focuses on ease of modification, rapid prototyping, and abstraction, making it useful for early-stage performance analysis and algorithm development.  

Both simulators serve different purposes:  
- The **Python-based simulator (py-libp2p + Shadow)** emphasizes ease of modification, flexibility, and rapid iteration for research.  
- The **Nim-based simulator (nim-libp2p + Shadow)** is closer to real-world implementations and performance tuning.  

By simulating DAS over Gossipsub in an Optimism context, this work contributes to the broader research on **scalable and efficient data availability solutions** for rollups and decentralized systems.  

## Acknowledgements  
The [Shadow Network Simulator](https://shadow.github.io/) was originally developed for the [Tor Project](https://www.torproject.org/).  

[py-libp2p](https://github.com/libp2p/py-libp2p) is an experimental implementation of libp2p in Python.  

This research is aligned with ongoing efforts in the Ethereum and Optimism ecosystems to enhance data availability and scalability.


# Key Reference: Data Availability Sampling (DAS) Simulator - Nim edition

Author: Csaba Kiraly, [Codex team](https://codex.storage/)

The goal of the simulator is to study and develop techniques for DAS (Data Availabilty Sampling). The simulator is composed of
- the Shadow Network Simulator: open-source framework to run real application code over a simulated network
- nim-libp2p: an experimental version of nim-libp2p code for DAS
- custom DAS application logic

This is our second simulator, the first being written in Python and available in the [DAS Research repo](https://github.com/codex-storage/das-research).

The two simulators have different levels of abstraction and serve different purposes:
- the [Python simulator]((https://github.com/codex-storage/das-research)) focuses on abstraction, scale, and ease of modification;
- this one, written in Nim, is much closer to a real implementation.

Both simulators aim to serve the development and comparative analysis of different DAS variants, including full DAS (with 2D encoding and blocks in the range of 32MB), but also PeerDAS, SubnetDAS and other variants.

## Acknowledgements

The [Shadow Network Simulator](https://shadow.github.io/) was originally developed for the [Tor project](https://www.torproject.org/).

[nim-libp2p](https://github.com/vacp2p/nim-libp2p) is in development and production use by the [Nimbus team](https://nimbus.team/).

The code of this library is based on the original work of the [Vac team](https://vac.dev/).

This work is supported by [grant FY22-0820 on Data Availability Sampling from the Ethereum Foundation](https://blog.codex.storage/codex-and-ethereum-foundation-kick-off/).

## Techniques and protocols supported

While many things are still work in progress, the simulation already has a large number of features implemented:

- configurable block size
- configurable segmentation, with optional 1D or 2D erasure coding (simulated)
- column/row based diffusion using GossipSub(-like) protocol
- cross-forwarding between rows and columns
- erasure coding based reconstruction (simulated), with forwarding and cross-forwarding
- DAS specific Gossipsub modifications (batch, rarest first, randomized order dispatch)

## Features in development
- ID-based custody and/or interest
- timout-based sampling protocol
- [LossyDAS, IncrementalDAS and DiDAS support](https://ethresear.ch/t/lossydas-lossy-incremental-and-diagonal-sampling-for-data-availability/18963)
- more Gossipsub modifications (duplicate supression, etc.)
- custom Diffusion protocol
- custom protocol unifying diffusion and sampling

# Usage

## Install Shadow

Follow [instructions for dependencies](https://shadow.github.io/docs/guide/install_dependencies.html) and for [installing the Shadow](https://shadow.github.io/docs/guide/install_shadow.html).

## Install Nim and Nimble

Follow [instructions to install Nim](https://nim-lang.org/install.html).

## Nim dependencies

Most dependencies will be installed in the `nimbledeps` folder, which enables easy tweaking.
nim-libp2p is included in the shadow/vendor folder as a git submodule.

```sh
nimble install -y unittest2@0.0.9 #workaround for bad nimble version resolution
nimble install -dy
```

## Run the simulator

```sh
cd shadow
./run.sh <runs> <FastNodes> <SlowNodes> [PacketLoss(float0..1)]
```
