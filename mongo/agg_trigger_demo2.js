exports = async function() {
  // Remove users that are not "testuser"
  const usersCollection = context.services.get("Cluster0").db("blogilista-app").collection("users");
  const user = await usersCollection.findOne({ "username": "testuser" });
  try {
    await usersCollection.deleteMany({ "_id": { $ne: user._id } });
  } catch (e) {
    console.log('Could not delete users', e);
  }
  // Remove extra blogs that do not belong to "testuser"
  const blogsCollection = context.services.get("Cluster0").db("blogilista-app").collection("blogs");
  try {
    await blogsCollection.deleteMany({ "user": { $ne: user._id } });
  } catch (e) {
    console.log('Could not delete blogs', e);
  }
  // Remove extra comments that do not belong to "testuser"
  const commentsCollection = context.services.get("Cluster0").db("blogilista-app").collection("comments");
  try {
    await commentsCollection.deleteMany({ "user": { $ne: user._id } });
  } catch (e) {
    console.log('Could not delete comments', e);
  }
  const leftBlogs = await blogsCollection.find();
  const leftComments =  await commentsCollection.find();
  const blogsArr = await leftBlogs.toArray();
  const commentsArr = await leftComments.toArray();
  // Remove the reference to deleted comments from blog documents
  blogsArr.forEach( b => {
    const commentsKeep = b.comments.filter(c => commentsArr.some( lf => lf._id.toString() === c.toString()));
    blogsCollection.updateOne(
    { _id: b._id}, 
    { $set: { comments: commentsKeep }},
    { $currentDate: { lastUpdated: false } }
    )
  });
};