// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/gopool"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	// This is the target size for the packs of transactions or announcements. A
	// pack can get larger than this if a single transactions exceeds this size.
	maxTxPacketSize = 100 * 1024
)

// blockPropagation is a block propagation event, waiting for its turn in the
// broadcast queue.
type blockPropagation struct {
	block *types.Block
	td    *big.Int
}

// broadcastBlocks is a write loop that multiplexes blocks and block accouncements
// to the remote peer. The goal is to have an async writer that does not lock up
// node internals and at the same time rate limits queued data.
func (p *Peer) broadcastBlocks() {
	fmt.Println("---------")
	fmt.Println("broadcastBlocks")
	for {
		select {
		case prop := <-p.queuedBlocks:
			fmt.Print("broadcastBlocks -1")
			if err := p.SendNewBlock(prop.block, prop.td); err != nil {
				return
			}
			p.Log().Trace("Propagated block", "number", prop.block.Number(), "hash", prop.block.Hash(), "td", prop.td)

		case block := <-p.queuedBlockAnns:
			fmt.Println("broadcastBlocks -5")
			if err := p.SendNewBlockHashes([]common.Hash{block.Hash()}, []uint64{block.NumberU64()}); err != nil {
				return
			}
			p.Log().Trace("Announced block", "number", block.Number(), "hash", block.Hash())

		case <-p.term:
			return
		}
	}
}

// broadcastTransactions is a write loop that schedules transaction broadcasts
// to the remote peer. The goal is to have an async writer that does not lock up
// node internals and at the same time rate limits queued data.
func (p *Peer) broadcastTransactions() {
	fmt.Println("---------")
	fmt.Println("broadcastTransactions")
	var (
		queue  []common.Hash         // Queue of hashes to broadcast as full transactions
		done   chan struct{}         // Non-nil if background broadcaster is running
		fail   = make(chan error, 1) // Channel used to receive network error
		failed bool                  // Flag whether a send failed, discard everything onward
	)
	for {
		// If there's no in-flight broadcast running, check if a new one is needed
		if done == nil && len(queue) > 0 {
			fmt.Println("broadcastTransactions - 1")
			// Pile transaction until we reach our allowed network limit
			var (
				hashesCount uint64
				txs         []*types.Transaction
				size        common.StorageSize
			)
			for i := 0; i < len(queue) && size < maxTxPacketSize; i++ {
				fmt.Println("broadcastTransactions - 2")
				fmt.Println(queue[i])
				fmt.Println(p.txpool.Get(queue[i]))
				if tx := p.txpool.Get(queue[i]); tx != nil {
					fmt.Println("broadcastTransactions - 2.1")
					txs = append(txs, tx)
					size += tx.Size()
				}
				hashesCount++
			}
			queue = queue[:copy(queue, queue[hashesCount:])]

			// If there's anything available to transfer, fire up an async writer
			fmt.Println("broadcastTransactions - 3")
			if len(txs) > 0 {
				fmt.Println("broadcastTransactions - 4")
				done = make(chan struct{})
				fmt.Println("+++++++++ chan struct")
				fmt.Println(done)
				go func() {
					fmt.Println("broadcastTransactions - 4.1")
					if err := p.SendTransactions(txs); err != nil {
						fmt.Println("broadcastTransactions - 4.....")
						fail <- err
						return
					}
					close(done)
					p.Log().Trace("Sent transactions", "count", len(txs))
				}()
			}
		}
		// Transfer goroutine may or may not have been started, listen for events
		select {
		case hashes := <-p.txBroadcast:
			// fmt.Println(p.txBroadcast)
			fmt.Println("broadcastTransactions - 7")
			fmt.Println(queue)
			fmt.Println("---------")
			// If the connection failed, discard all transaction events
			if failed {
				fmt.Println("broadcastTransactions - 7.1")
				continue
			}

			// New batch of transactions to be broadcast, queue them (with cap)
			queue = append(queue, hashes...)
			fmt.Println("---------")
			fmt.Println(hashes)
			fmt.Println(queue)
			fmt.Println("---------")

			if len(queue) > maxQueuedTxs {
				fmt.Println("broadcastTransactions - 8")
				// Fancy copy and resize to ensure buffer doesn't grow indefinitely
				queue = queue[:copy(queue, queue[len(queue)-maxQueuedTxs:])]
			}

		case <-done:
			done = nil

		case <-fail:
			failed = true

		case <-p.txTerm:
			return

		case <-p.term:
			return
		}
	}
}

// announceTransactions is a write loop that schedules transaction broadcasts
// to the remote peer. The goal is to have an async writer that does not lock up
// node internals and at the same time rate limits queued data.
func (p *Peer) announceTransactions() {
	fmt.Println("---------")
	fmt.Println("announceTransactions")
	var (
		queue  []common.Hash         // Queue of hashes to announce as transaction stubs
		done   chan struct{}         // Non-nil if background announcer is running
		fail   = make(chan error, 1) // Channel used to receive network error
		failed bool                  // Flag whether a send failed, discard everything onward
	)
	for {
		// If there's no in-flight announce running, check if a new one is needed
		if done == nil && len(queue) > 0 {
			fmt.Println("announceTransactions - 1")
			// Pile transaction hashes until we reach our allowed network limit
			var (
				count   int
				pending []common.Hash
				size    common.StorageSize
			)
			for count = 0; count < len(queue) && size < maxTxPacketSize; count++ {
				fmt.Println("announceTransactions - 2")
				if p.txpool.Get(queue[count]) != nil {
					fmt.Println("announceTransactions - 3")
					pending = append(pending, queue[count])
					size += common.HashLength
				}
			}
			// Shift and trim queue
			queue = queue[:copy(queue, queue[count:])]

			// If there's anything available to transfer, fire up an async writer
			if len(pending) > 0 {
				fmt.Println("announceTransactions - 4")
				done = make(chan struct{})
				gopool.Submit(func() {
					if err := p.sendPooledTransactionHashes(pending); err != nil {
						fail <- err
						return
					}
					close(done)
					//p.Log().Trace("Sent transaction announcements", "count", len(pending))
				})
			}
		}
		// Transfer goroutine may or may not have been started, listen for events
		select {
		case hashes := <-p.txAnnounce:
			// If the connection failed, discard all transaction events
			if failed {
				continue
			}
			// New batch of transactions to be broadcast, queue them (with cap)
			queue = append(queue, hashes...)
			fmt.Println("announceTransactions - 6")

			if len(queue) > maxQueuedTxAnns {
				fmt.Println("announceTransactions - 7")
				// Fancy copy and resize to ensure buffer doesn't grow indefinitely
				queue = queue[:copy(queue, queue[len(queue)-maxQueuedTxAnns:])]
			}

		case <-done:
			done = nil

		case <-fail:
			failed = true

		case <-p.txTerm:
			return

		case <-p.term:
			return
		}
	}
}
