#!/bin/bash

# Quick test script to see what hep_sub outputs
# This helps debug the job ID parsing

echo "=== Testing hep_sub command ==="
echo ""

# Create a simple test job
cat > test_job.sh << 'EOF'
#!/bin/bash
echo "Hello from test job"
hostname
date
sleep 5
EOF

chmod +x test_job.sh

echo "Submitting test job with: hep_sub -g cms test_job.sh"
echo ""
echo "--- Output from hep_sub: ---"
hep_sub -g cms test_job.sh 2>&1 | tee hepsub_output.txt
echo ""
echo "--- End of output ---"
echo ""

if [ -f hepsub_output.txt ]; then
    echo "Output saved to: hepsub_output.txt"
    echo ""
    echo "Please check what the output format is and share it!"
    echo "This will help fix the job ID regex pattern."
fi

# Cleanup
rm -f test_job.sh
