package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/iotaledger/sandbox/auth"

	uuid "github.com/satori/go.uuid"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "admin",
		Short: "admin is a simple cli interface for the iota sandbox",
	}
)

var (
	credPath  = ""
	projectID = ""
)

var (
	authEMail  = ""
	authActive = true
)

func init() {
	rootCmd.PersistentFlags().StringVar(&credPath, "credentials", "./credentials.json", "google cloud credentials")
	rootCmd.PersistentFlags().StringVar(&projectID, "project-id", "", "google cloud project id")
}

func main() {
	var authStoreCmd = &cobra.Command{
		Use:   "auth",
		Short: "interact with the auth storage",
	}

	authStoreCmd.PersistentFlags().StringVarP(&authEMail, "email", "", "", "email address to store along with the token")
	authStoreCmd.PersistentFlags().BoolVarP(&authActive, "active", "", true, "is the token enabled or not")

	var authStoreAddCmd = &cobra.Command{
		Use:   "add",
		Short: "add a new auth store entry, and prints token if created successfully",
		Example: `admin auth add cd1bf18a-1f8a-4d0e-ad87-dd52739c567f --active=true --email=foo@bar.com --project-id=sandbox
# if you don't provide a token then a uuidv4 will be used
admin auth add --project-id=sandbox
		`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunAddAuth(cmd, args)
		},
	}

	var authStoreGetCmd = &cobra.Command{
		Use:     "get",
		Short:   "retrieve auth store entry with supplied token",
		Example: `admin auth get cd1bf18a-1f8a-4d0e-ad87-dd52739c567f --project-id=sandbox`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunGetAuth(cmd, args)
		},
	}

	authStoreCmd.AddCommand(authStoreAddCmd)
	authStoreCmd.AddCommand(authStoreGetCmd)
	rootCmd.AddCommand(authStoreCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func RunAddAuth(cmd *cobra.Command, args []string) error {
	if err := rootCmd.ParseFlags(args); err != nil {
		return err
	}

	ds, err := auth.NewGCloudDataStore(projectID, credPath)
	if err != nil {
		return err
	}

	var tok string
	if len(args) < 1 {
		tok = uuid.NewV4().String()
	} else {
		tok = args[0]
	}

	a := &auth.Auth{
		Token:      tok,
		Active:     authActive,
		EMail:      authEMail,
		CreatedAt:  time.Now().Unix(),
		ModifiedAt: time.Now().Unix(),
	}

	ctx, cf := context.WithTimeout(context.Background(), time.Second*15)
	defer cf()

	if err := ds.Add(ctx, a); err != nil {
		return err
	}

	fmt.Printf("%s\n", a.Token)
	return nil
}

func RunGetAuth(cmd *cobra.Command, args []string) error {
	if err := rootCmd.ParseFlags(args); err != nil {
		return err
	}

	ds, err := auth.NewGCloudDataStore(projectID, credPath)
	if err != nil {
		return err
	}

	if len(args) < 1 {
		return fmt.Errorf("no token supplied")
	}
	tok := args[0]

	ctx, cf := context.WithTimeout(context.Background(), time.Second*15)
	defer cf()

	a, err := ds.Get(ctx, tok)
	if err != nil {
		return err
	}

	fmt.Printf("%#v\n", a)
	return nil
}
