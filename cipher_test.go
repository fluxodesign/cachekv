package cachekv

import (
	"log"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	alternateDir = "./test-alternate"
)

func cipherSetup() func() {
	var err error
	KeyPath = alternateDir
	return func() {
		err = os.RemoveAll(alternateDir)
		if err != nil {
			log.Println("error removing test alternate directory")
		}
	}
}

func TestGenKeypair(t *testing.T) {
	defer cipherSetup()()
	privatePath := path.Join(alternateDir, privateFile)
	publicPath := path.Join(alternateDir, publicFile)
	assert.Nil(t, genKeypair())
	info, err := os.Stat(privatePath)
	assert.Nil(t, err)
	fMode := info.Mode()
	assert.True(t, fMode&os.FileMode(0600) != 0)
	info, err = os.Stat(publicPath)
	assert.Nil(t, err)
	fMode = info.Mode()
	assert.True(t, fMode&os.FileMode(0644) != 0)
}

func TestGenKeypairAlreadyExists(t *testing.T) {
	defer cipherSetup()()
	privatePath := path.Join(alternateDir, privateFile)
	publicPath := path.Join(alternateDir, publicFile)
	assert.Nil(t, genKeypair())
	_, err := os.Stat(privatePath)
	assert.Nil(t, err)
	_, err = os.Stat(publicPath)
	assert.Nil(t, err)
	// second try
	assert.NotNil(t, genKeypair())
}

func TestCheckPrivateAndPublicKeys(t *testing.T) {
	defer cipherSetup()()
	assert.Nil(t, genKeypair())
	privateKey, publicKey, err := readFromStorage(alternateDir)
	assert.Nil(t, err)
	assert.NotNil(t, privateKey)
	assert.NotNil(t, publicKey)
	assert.True(t, privateKey.PublicKey.Equal(publicKey))
}

func TestEncryptDecryptMessage(t *testing.T) {
	defer cipherSetup()()
	assert.Nil(t, genKeypair())
	sharedValue := []byte("34534532|34t4645")
	message := []byte("Hello,World! This is me!")
	encrypted, err := encryptMessage(message, sharedValue)
	assert.Nil(t, err)
	decrypted, err := decryptMessage(encrypted, sharedValue)
	assert.Nil(t, err)
	assert.Equal(t, message, decrypted)
}

func TestHashFile(t *testing.T) {
	defer cipherSetup()()
	assert.Nil(t, genKeypair())
	privatePath := path.Join(alternateDir, privateFile)
	publicPath := path.Join(alternateDir, publicFile)
	hash, err := hashFile(privatePath)
	assert.Nil(t, err)
	assert.NotNil(t, hash)
	secondHash, err := hashFile(privatePath)
	assert.Nil(t, err)
	assert.NotNil(t, secondHash)
	assert.Equal(t, hash, secondHash)
	hash, err = hashFile(publicPath)
	assert.Nil(t, err)
	assert.NotNil(t, hash)
	secondHash, err = hashFile(publicPath)
	assert.Nil(t, err)
	assert.NotNil(t, secondHash)
	assert.Equal(t, hash, secondHash)
}

func TestExtractString(t *testing.T) {
	var runes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	var paddings = []rune("#$%&^*0@!")
	randomRunes := make([]rune, 64)
	for i := range randomRunes {
		randomRunes[i] = runes[rand.Intn(len(runes))]
	}
	randomString := string(randomRunes)
	cutTo32 := randomString[:32]
	extracted, err := extractString("", -32)
	assert.NotNil(t, err)
	assert.Equal(t, "", extracted)
	extracted, err = extractString("", 32)
	assert.Nil(t, err)
	allPaddings := true
	for _, r := range extracted {
		matches := false
		for _, j := range paddings {
			if r == j {
				matches = true
				break
			}
		}
		if !matches {
			allPaddings = false
		}
	}
	assert.True(t, allPaddings)
	extracted, err = extractString(randomString, 32)
	assert.Nil(t, err)
	assert.Equal(t, cutTo32, extracted)
}
