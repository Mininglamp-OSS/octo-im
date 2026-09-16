package store

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func TestDeleteConversationAsyncProposesWithoutWaitingForApply(t *testing.T) {
	slot := &recordingSlot{slotID: 29}
	store := New(NewOptions(WithSlot(slot)))

	if err := store.DeleteConversationAsync("uid-1", "channel-1", 2); err != nil {
		t.Fatalf("DeleteConversationAsync() error = %v", err)
	}

	if slot.proposeCalls != 1 {
		t.Fatalf("Propose() calls = %d, want 1", slot.proposeCalls)
	}
	if slot.proposeUntilAppliedCalls != 0 {
		t.Fatalf("ProposeUntilApplied() calls = %d, want 0", slot.proposeUntilAppliedCalls)
	}
	if slot.proposedSlotID != 29 {
		t.Fatalf("Propose() slot ID = %d, want 29", slot.proposedSlotID)
	}

	cmd := &CMD{}
	if err := cmd.Unmarshal(slot.proposedData); err != nil {
		t.Fatalf("unmarshal proposed command: %v", err)
	}
	if cmd.CmdType != CMDDeleteConversation {
		t.Fatalf("command type = %v, want %v", cmd.CmdType, CMDDeleteConversation)
	}
	uid, channelID, channelType, err := cmd.DecodeCMDDeleteConversation()
	if err != nil {
		t.Fatalf("decode proposed command: %v", err)
	}
	if uid != "uid-1" || channelID != "channel-1" || channelType != 2 {
		t.Fatalf("decoded command = (%q, %q, %d), want (%q, %q, %d)", uid, channelID, channelType, "uid-1", "channel-1", 2)
	}
}

func TestDeleteConversationAsyncReturnsProposalError(t *testing.T) {
	wantErr := errors.New("proposal queue unavailable")
	slot := &recordingSlot{slotID: 29, proposeErr: wantErr}
	store := New(NewOptions(WithSlot(slot)))

	err := store.DeleteConversationAsync("uid-1", "channel-1", 2)
	if !errors.Is(err, wantErr) {
		t.Fatalf("DeleteConversationAsync() error = %v, want %v", err, wantErr)
	}
}

type recordingSlot struct {
	slotID                   uint32
	proposeCalls             int
	proposeUntilAppliedCalls int
	proposedSlotID           uint32
	proposedData             []byte
	proposeErr               error
}

func (s *recordingSlot) SlotLeaderId(uint32) uint64 { return 1 }

func (s *recordingSlot) GetSlotId(string) uint32 { return s.slotID }

func (s *recordingSlot) Propose(slotID uint32, data []byte) (*types.ProposeResp, error) {
	s.proposeCalls++
	s.proposedSlotID = slotID
	s.proposedData = append([]byte(nil), data...)
	return &types.ProposeResp{}, s.proposeErr
}

func (s *recordingSlot) ProposeUntilApplied(uint32, []byte) (*types.ProposeResp, error) {
	s.proposeUntilAppliedCalls++
	return &types.ProposeResp{}, nil
}

func (s *recordingSlot) ProposeUntilAppliedTimeout(context.Context, uint32, []byte) (*types.ProposeResp, error) {
	s.proposeUntilAppliedCalls++
	return &types.ProposeResp{}, nil
}
