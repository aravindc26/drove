package util

import (
	"context"
	"fmt"
	"time"
)

func InitializeJobRepository() *JobRepository {
	repo := CreateJobRepository()
	repo.addJob("test", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test1")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi")
			}
		}
	})

	repo.addJob("test2", func(ctx context.Context) bool {
		fmt.Println("hi2")
		return true
	})

	repo.addJob("test3", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test3")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi3")
			}
		}
	})

	repo.addJob("test4", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test4")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi4")
			}
		}
	})

	repo.addJob("test5", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test5")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi5")
			}
		}
	})

	repo.addJob("test6", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test6")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi6")
			}
		}
	})

	repo.addJob("test7", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test7")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi7")
			}
		}
	})

	repo.addJob("test8", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test8")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi8")
			}
		}
	})

	repo.addJob("test9", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test9")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi9")
			}
		}
	})

	repo.addJob("test10", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test10")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi10")
			}
		}
	})

	repo.addJob("test11", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test11")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi11")
			}
		}
	})

	repo.addJob("test12", func(ctx context.Context) bool {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exiting test12")
				return false
			case <-time.Tick(time.Second):
				fmt.Println("hi12")
			}
		}
	})

	return repo
}
