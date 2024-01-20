git fetch --prune --unshallow 2>/dev/null
CURRENT_VERSION=`git describe --abbrev=0 --tags 2>/dev/null`

if [[ $CURRENT_VERSION == '' ]]
then    
    CURRENT_VERSION='v0.1.0'
fi

echo "Current Version: $CURRENT_VERSION"

if git log --grep=major
then  
    VERSION='MAJOR'
elif git log --grep=minor
then
    VERSION='MINOR'
elif git log --grep=patch
then
    VERSION='PATCH'
else
    echo "the commit doesn't have any version type"
fi

echo 'version is' $VERSION
# split the current version

CURRENT_VERSION_PARTS=(${ CURRENT_VERSION//./ })
VNUM1=${CURRENT_VERSION_PARTS[0]}
VNUM2=${CURRENT_VERSION_PARTS[1]}
VNUM3=${CURRENT_VERSION_PARTS[2]}

if [[ $VERSION=="MAJOR" ]]
    then 
    VNUM_NUMERICAL_PART="${VNUM1#v}"
    VNUM1="v$(($VNUM_NUMERICAL_PART+1))"
elif [ [ $VERSION=="MINOR" ] ]
    then 
    VNUM2=$(($VUM2+1))
elif [[ $VERSION=="PATCH "]]
    then 
    VNUM3=$(($VNUM3+1))
else 
    echo "Specify the commit does not have a version type"

NEW_TAG="$VNUM1:$VNUM2:$VNUM3"



echo 'Current version is set to:' $NEW_TAG

#get the hash and check if it has a tag
GIT_COMMIT=`git rev-parse HEAD`
NEEDS_TAG=`git describe --contains $GIT_COMMIT 2>/dev/null`

#only tag if no tag already
if [-z "$NEEDS_TAG"]; then
    echo "Tagged with $NEW_TAG"
    git tag $NEW_TAG
    git push --tags
    git push 
else
echo "Already a tag on this commit"
fi

echo ::set-output name=git-tag::$NEW_TAG

exit 0





