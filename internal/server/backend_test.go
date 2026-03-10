package server

import "testing"

func TestValidateBalancerType(t *testing.T) {
	if err := validateBalancerType(BalancerTypeRoundRobin); err != nil {
		t.Fatalf("round_robin should be valid: %v", err)
	}
	if err := validateBalancerType(BalancerTypeFailOver); err != nil {
		t.Fatalf("fail_over should be valid: %v", err)
	}
	if err := validateBalancerType(BalancerType("broken")); err == nil {
		t.Fatal("expected invalid balancer type error")
	}
}
